(*
   This merges multiple log files into one file, sorted by timestamp.
   It can start from a previous position, for periodic incremental merging.
*)

let log_merge_folder = Log.log_folder ^ "/merged"
let log_merge_output = log_merge_folder ^ "/api.log"
let positions_file = log_merge_folder ^ "/positions.json"

(* We'll use a (filename, entry list) Hashtbl.t LogEntries.t,
   where type filename = string and entry = string.
   So, a LogEntries.t maps timestamps to the ordered entries from each file
   which share that timestamp, and it keeps everything ordered by timestamp.
   Each entry starts with the line containing its timestamp, plus any
   following lines until the next timestamped line.
*)
module LogEntries = Map.Make(struct
  type t = Nldate.t
  let compare = Pervasives.compare
end)

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

(* This is a new entry for the given timestamp, so add it to the table. *)
let add_new_log_entry ts_tbl filename entry =
  try
    let entries_from_same_file = Hashtbl.find ts_tbl filename in
    (* Keep entries with same ts and file in reverse order;
       will call List.rev when printing the log *)
    Hashtbl.replace ts_tbl filename (entry :: entries_from_same_file)
  with Not_found ->
    (* First entry for this ts in this file *)
    Hashtbl.add ts_tbl filename [entry]

(* This is a continuation of a previous timestamped entry, so append it. *)
let append_to_previous_entry ts_tbl filename line =
  match Hashtbl.find ts_tbl filename with
  | [] -> assert false (* Log_merge.parse ensures at least one entry *)
  | last_timestamped_entry :: rest ->
      let appended_entry = last_timestamped_entry ^ "\n" ^ line in
      Hashtbl.replace ts_tbl filename (appended_entry :: rest)

(* This is the first entry for this timestamp in any file parsed so far,
   so create a new table containing it. *)
let create_ts_table filename entry =
  let entries_by_file = Hashtbl.create 8 in
  Hashtbl.add entries_by_file filename [entry];
  entries_by_file

(* Remove the last entry and subtract its length from the input position
   (with an extra - 1 for the newline omitted by input_line).
   We need to do this at the end of parsing, because the last entry could be
   continued on subsequent lines without timestamps, which would make our next
   parsing round fail (log parsing must always begin with a timestamp).
*)
let rewind_one_entry log filename input last_known_ts =
  let entries_with_last_ts = LogEntries.find last_known_ts log in
  match Hashtbl.find entries_with_last_ts filename with
  | [] -> assert false (* Log_merge.parse ensures at least one entry *)
  | last_timestamped_entry :: rest ->
      Hashtbl.replace entries_with_last_ts filename rest;
      (* This becomes the final result of Log_merge.parse_until_end *)
      (log, pos_in input - String.length last_timestamped_entry - 1)

(* Main parsing loop *)
let rec parse_until_end log filename input last_known_ts =
  match next_line input with
  | None ->
      (* Reached EOF; rewind to just before the last timestamp seen *)
      rewind_one_entry log filename input last_known_ts
  | Some line ->
      (match parse_timestamp line with
      | None ->
          (* Continuation of previous log entry *)
          let entries_with_last_ts = LogEntries.find last_known_ts log in
          append_to_previous_entry entries_with_last_ts filename line;
          parse_until_end log filename input last_known_ts
      | Some ts ->
          (* Start of new log entry *)
          (try
            let entries_with_same_ts = LogEntries.find ts log in
            add_new_log_entry entries_with_same_ts filename line;
            parse_until_end log filename input ts
          with Not_found ->
            (* First log entry seen for this timestamp *)
            let entries_by_file = create_ts_table filename line in
            let updated_log = LogEntries.add ts entries_by_file log in
            parse_until_end updated_log filename input ts
          )
      )

(* Parse log entries from the given filename starting at position
   into the LogEntries.t log. Return the updated log, along with a
   new position from which we can resume parsing when more data is
   available.
*)
let parse log filename position =
  let input = open_in filename in
  seek_in input position; (* TODO Handle rotated log case? *)
  match next_line input with
  | None ->
      (* No new data to parse *)
      close_in input;
      (log, position)
  | Some first_line ->
      (* Make sure we're starting from a valid log entry,
         then call parse_until_end to finish consuming the file *)
      (match parse_timestamp first_line with
      | None ->
          failwith (
            Printf.sprintf
              "Error parsing log file %s starting at position %d: \
               expected timestamped line, got:\n%s"
              filename position first_line
          )
      | Some first_ts ->
          let entries_by_file = create_ts_table filename first_line in
          Hashtbl.add entries_by_file filename [first_line];
          let log_with_parsed_input =
            let updated_log = LogEntries.add first_ts entries_by_file log in
            (* Enter main parsing loop *)
            parse_until_end updated_log filename input first_ts
          in
          close_in input;
          log_with_parsed_input
      )

(* Given a list of (filename, start_position) pairs,
   parse all the files into the LogEntries.t log.
   Return the updated log, along with a list of
   (filename, end_position) pairs.
*)
let merge log files =
  List.fold_right (fun (filename, start_pos) (log, ends) ->
    let (log, end_pos) = parse log filename start_pos in
    (log, (filename, end_pos) :: ends)
  ) files (log, [])

(* Print the entries from the LogEntries.t log in timestamp sorted order.
   When multiple files have entries with the same timestamp, print the entries
   in the order they appeared in their file, and process files in the order
   given by filenames.
*)
let print log filenames output =
  LogEntries.iter (fun _ entries_by_file ->
    List.iter (fun filename ->
      try
        let entries = Hashtbl.find entries_by_file filename in
        List.iter (fun entry ->
          BatIO.nwrite output (entry ^ "\n")
        ) (List.rev entries)
      with Not_found -> ()
    ) filenames
  ) log

(* Replace the first occurrence of (key, _) in lst with (key, value).
   If no binding is found, create one. *)
let rec replace_assoc key value = function
  | [] -> [(key, value)]
  | (k, v) :: rest ->
      if k = key then (key, value) :: rest
      else (k, v) :: replace_assoc key value rest

let merge_streams cmp init fold l =
  match l with
      [] -> Stream.from (fun _ -> None)
    | l ->
        let next i =
          let opt =
            List.fold_left (
              fun opt st ->
                match opt, Stream.peek st with
                    _, None -> opt
                  | Some key0, Some (key, _) ->
                      let c = cmp key0 key in
                      if c <= 0 then opt
                      else Some key
                  | None, Some (key, _) ->
                      Some key
            ) None l
          in
          match opt with
              None -> None
            | Some key ->
                let accu = init key in
                let result =
                  List.fold_left (
                    fun accu st ->
                      match Stream.peek st with
                          Some (k, v) ->
                            if cmp key k = 0 then (
                              Stream.junk st;
                              fold accu v
                            )
                            else
                              accu
                        | None ->
                            accu
                  ) accu l
                in
                Some (key, result)
        in
        Stream.from next
;;

(* Return a (timestamp, line) Stream.t
   made up of lines from input_path starting at position bytes,
   with a timestamp key for merging with other streams.
*)
(* We number each input file and make that a secondary part of the
   timestamp, so that after merging, lines from the same file that
   share a timestamp are kept together. *)
let mergeable_stream fileno (input_path, position) =
  let input = open_in input_path in
  seek_in input position;
  let end_pos = ref 0 in
  let entry = ref None in
  let finished_previous_entry new_data =
    let prev_entry = !entry in
    entry := new_data;
    prev_entry
  in
  let stream =
    Stream.from (fun _ ->
      match next_line input with
      | None ->
          end_pos := pos_in input;
          finished_previous_entry None
      | Some line ->
          let rec read_more_lines () =
            match next_line input with
            | None ->
                end_pos := pos_in input;
                finished_previous_entry None
            | Some line ->
                (match parse_timestamp line with
                | None ->
                    (* Must be a continuation of the previous entry *)
                    (match !entry with
                    | None -> assert false
                    | Some ((ts, fileno), lines) ->
                        entry := Some ((ts, fileno), lines ^ "\n" ^ line);
                        read_more_lines ()
                    )
                | Some ts ->
                    (* Start of a new entry, which means the previous one
                       is now completed. Return it, and save the new data
                       for the next round. *)
                    finished_previous_entry (Some ((ts, fileno), line))
                )
          in
          (match parse_timestamp line with
          | None ->
              (* No timestamp means this line is a continuation of the
                 previous entry. Keep reading lines until we complete the
                 entry or run out of data to read. *)
              (match !entry with
              | None -> assert false
              | Some ((ts, fileno), lines) ->
                  entry := Some ((ts, fileno), lines ^ "\n" ^ line);
                  read_more_lines ()
              )
          | Some ts ->
              (* New line starting with a timestamp. Use it and save it as the
                 most recent previous timestamp. *)
              (match !entry with
              | None ->
                  entry := Some ((ts, fileno), line);
                  read_more_lines ()
              | Some prev_entry ->
                  entry := Some ((ts, fileno), line);
                  Some prev_entry
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
  let input_streams = List.map (fun (s, i, e) ->
    Stream.from (fun _ ->
      match Stream.npeek 2 s with
      | [] -> None
      | [last] ->
          let (_, final) = Stream.next s in
          e := !e - String.length final - 1;
          None
      | _ -> Some (Stream.next s)
    ), i, e
  ) input_streams in
  let init _ = "" in
  let fold accu line = if accu = "" then line else accu ^ "\n" ^ line in
  let data (stream, _, _) = stream in
  let merged =
    merge_streams compare init fold (List.map data input_streams)
  in
  Stream.iter (fun (_, line) ->
    BatIO.nwrite to_output (line ^ "\n")
  ) merged;
  (* Save our end positions to the JSON file *)
  let end_positions =
    List.fold_left (fun positions (_, filename, end_pos) ->
      replace_assoc filename (`Int !end_pos) positions
    ) (to_assoc start_positions) input_streams
  in
  let positions_file = open_out at_positions in
  let json_data = Yojson.Basic.to_string (`Assoc end_positions) in
  output_string positions_file (json_data ^ "\n");
  close_out positions_file
 
  (*
  (* Merge from the start positions and print the result *)
  (*
  let log = LogEntries.empty in
  let (merged, end_inputs) = merge log positioned_inputs in
  print merged (List.map fst end_inputs) to_output;
  *)
  let (merged, end_inputs) = assert false in

  (* Save our end positions to the JSON file *)
  let end_positions =
    List.fold_left (fun positions (filename, end_pos) ->
      replace_assoc filename (`Int end_pos) positions
    ) (to_assoc start_positions) end_inputs
  in
  let positions_file = open_out at_positions in
  let json_data = Yojson.Basic.to_string (`Assoc end_positions) in
  output_string positions_file (json_data ^ "\n");
  close_out positions_file
  *)

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
