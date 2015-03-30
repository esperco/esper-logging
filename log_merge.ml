(*
   This merges multiple log files into one file, sorted by timestamp.
   It can start from a previous position, for periodic incremental merging.
*)

let log_merge_folder = Log.log_folder ^ "/merged"
let log_merge_output = log_merge_folder ^ "/api.log"
let positions_file = log_merge_folder ^ "/positions.json"

(* I don't like exceptions unless I need to unwind the stack. And I don't. *)
let next_line input =
  try Some (input_line input)
  with End_of_file -> None

(* Extract timestamp from lines formatted like "^[timestamp] ...$" *)
let parse_timestamp line =
  try
    let subs = Pcre.exec ~pat:"^\\[([^]]*)\\]" line in
    Some (Nldate.parse (Pcre.get_substring subs 1))
  with
  | Not_found -> None
  | Invalid_argument "Netdate.parse" -> failwith ("Corrupt timestamp: " ^ line)

(* Replace the first occurrence of (key, _) in lst with (key, value).
   If no binding is found, create one. *)
let rec replace_assoc key value = function
  | [] -> [(key, value)]
  | (k, v) :: rest ->
      if k = key then (key, value) :: rest
      else (k, v) :: replace_assoc key value rest

(* Return a (timestamp, line) Stream.t
   made up of lines from input_path starting at position bytes,
   with a timestamp key for merging with other streams.

   We number each input file and make that a secondary part of the
   timestamp, so that after merging, lines from the same file that
   share a timestamp are kept together.
*)
let mergeable_stream fileno (input_path, position) =
  let input = open_in input_path in
  seek_in input position;
  (* Final position will be set at EOF *)
  let end_pos = ref 0 in
  let entry = ref None in
  (* Save new_data as start of new entry, return previous entry *)
  let shift new_data =
    let prev_entry = !entry in
    entry := new_data;
    prev_entry
  in
  (* Append lines to the current entry until the next timestamp or EOF *)
  let rec read_more_lines () =
    match next_line input with
    | None ->
        (* EOF *)
        end_pos := pos_in input;
        shift None
    | Some line ->
        (match parse_timestamp line with
        | None ->
            (* Continuation of previous entry *)
            continue line
        | Some ts ->
            (* Start of a new entry, previous one is now complete *)
            shift (Some ((ts, fileno), line))
        )
  (* Continue the current entry by appending line *)
  and continue line =
    match !entry with
    | None ->
        (* Missing initial timestamp, corrupt log or bad position *)
        failwith (
          let position = pos_in input - String.length line - 1 in
          Printf.sprintf
            "Error parsing log file %s at position %d: \
             expected timestamped line, got:\n%s"
            input_path position line
        )
    | Some ((ts, fileno), rest) ->
        entry := Some ((ts, fileno), rest ^ "\n" ^ line);
        read_more_lines ()
  in
  let stream =
    Stream.from (fun _ ->
      match next_line input with
      | None ->
          (* EOF *)
          end_pos := pos_in input;
          shift None
      | Some line ->
          (match parse_timestamp line with
          | None ->
              (* Continuation of previous entry *)
              continue line
          | Some ts ->
              (* Start of a new entry, perhaps the very first one *)
              (match !entry with
              | None ->
                  (* For the first entry, we have nothing previous to return,
                     so we must keep reading until the entry is complete *)
                  entry := Some ((ts, fileno), line);
                  read_more_lines ()
              | prev_entry ->
                  entry := Some ((ts, fileno), line);
                  prev_entry
              )
          )
    )
  in
  (stream, input_path, end_pos)

let append_new_entries ~from_inputs ~at_positions ~to_output =
  let open Yojson.Basic.Util in
  (* Read start positions from JSON file *)
  let positions_file = open_in_gen [Open_creat] 0o666 at_positions in
  let json_data = try input_line positions_file with End_of_file -> "" in
  close_in positions_file;
  let start_positions =
    if json_data = "" then `Assoc []
    else Yojson.Basic.from_string json_data
  in
  (* Look up the position for each input file *)
  let positioned_inputs =
    List.map (fun input_path ->
      match member input_path start_positions |> to_int_option with
      | None -> (input_path, 0)
      | Some i -> (input_path, i)
    ) from_inputs
  in
  (* Merge from the start positions and print the result *)
  let input_streams = List.mapi mergeable_stream positioned_inputs in
  let truncated_streams =
    List.map (fun (stream, input_path, end_pos) ->
      (* Drop the last entry from stream and update end_pos accordingly *)
      let truncated =
        Stream.from (fun _ ->
          match Stream.npeek 2 stream with
          | [] -> None
          | [_] ->
              let (_, last) = Stream.next stream in
              end_pos := !end_pos - String.length last - 1;
              None
          | [_; _] -> Some (Stream.next stream)
          | _ -> assert false
        )
      in
      (truncated, input_path, end_pos)
    ) input_streams
  in
  let init _ = "" in
  let fold accu line = if accu = "" then line else accu ^ "\n" ^ line in
  let data (stream, _, _) = stream in
  let merged =
    Util_stream.merge compare init fold (List.map data truncated_streams)
  in
  Stream.iter (fun (_, line) ->
    BatIO.nwrite to_output (line ^ "\n")
  ) merged;
  (* Save our end positions to the JSON file *)
  let end_positions =
    List.fold_left (fun positions (_, filename, end_pos) ->
      replace_assoc filename (`Int !end_pos) positions
    ) (to_assoc start_positions) truncated_streams
  in
  let positions_file = open_out at_positions in
  let json_data = Yojson.Basic.to_string (`Assoc end_positions) in
  output_string positions_file (json_data ^ "\n");
  close_out positions_file

let main ~offset =
  let dirlist = Array.to_list (Sys.readdir Log.log_folder) in
  let inputs =
    BatList.filter_map (fun filename ->
      if BatString.exists filename ".log."
      then Some (Log.log_folder ^ "/" ^ filename)
      else None
    ) dirlist
  in
  let output =
    BatFile.open_out
      ~mode:[`create; `append]
      ~perm:(BatFile.unix_perm 0o666)
      log_merge_output
  in
  append_new_entries
    ~from_inputs:inputs
    ~at_positions:positions_file
    ~to_output:output;
  BatIO.close_out output

module Test = struct
  let test_log_merge () =
    (* Create two temp files, write some sample log data *)
    let foo_path = "/tmp/foo.test.log" in
    let bar_path = "/tmp/bar.test.log" in
    let foo_data_1 = "\
[2015-02-19T10:29:54.482-08:00] foo line 1
[2015-02-19T10:30:28.137-08:00] foo line 2, continued below
continuation of foo line 2
[2015-02-19T10:31:27.003-08:00] foo line 3\n" in
    let bar_data_1 = "\
[2015-02-19T10:29:54.528-08:00] bar line 1
[2015-02-19T10:30:28.137-08:00] bar line 2
[2015-02-19T10:32:03.423-08:00] bar line 3, continued below\n" in
    let foo_out = open_out foo_path in
    let bar_out = open_out bar_path in
    output_string foo_out foo_data_1; flush foo_out;
    output_string bar_out bar_data_1; flush bar_out;

    (* Merge the first round of log data *)
    let positions_path = "/tmp/positions.json" in
    (try Unix.unlink positions_path with Unix.Unix_error _ -> ()); (* rm *)
    let string_buffer = BatIO.output_string () in
    append_new_entries
      ~from_inputs:[foo_path; bar_path]
      ~at_positions:positions_path
      ~to_output:string_buffer;

    (* Append some more log data to the temp files *)
    let foo_data_2 = "\
[2015-02-19T10:31:27.003-08:00] foo line 4
[2015-02-19T10:33:42.921-08:00] foo line 5
[2015-02-19T10:45:54.162-08:00] foo line 6\n" in
    let bar_data_2 = "\
continuation of bar line 3 part 1/2
continuation of bar line 3 part 2/2
[2015-02-19T10:34:12.456-08:00] bar line 4\n" in
    output_string foo_out foo_data_2; close_out foo_out;
    output_string bar_out bar_data_2; close_out bar_out;

    (* Merge the second round, starting from where the first left off *)
    append_new_entries
      ~from_inputs:[foo_path; bar_path]
      ~at_positions:positions_path
      ~to_output:string_buffer;

    (* The result should be sorted and missing the last line of each file *)
    let result = BatIO.close_out string_buffer in
    let expected = "\
[2015-02-19T10:29:54.482-08:00] foo line 1
[2015-02-19T10:29:54.528-08:00] bar line 1
[2015-02-19T10:30:28.137-08:00] foo line 2, continued below
continuation of foo line 2
[2015-02-19T10:30:28.137-08:00] bar line 2
[2015-02-19T10:31:27.003-08:00] foo line 3
[2015-02-19T10:31:27.003-08:00] foo line 4
[2015-02-19T10:32:03.423-08:00] bar line 3, continued below
continuation of bar line 3 part 1/2
continuation of bar line 3 part 2/2
[2015-02-19T10:33:42.921-08:00] foo line 5\n" in
    result = expected

  let tests = [("test_log_merge", test_log_merge)]
end

let tests = Test.tests
