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
    let subs = Pcre.exec ~pat:"^\\[(\\d[0-9T.:+-]*)\\]" line in
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
let mergeable_stream fileno (input_path, finalized, position) =
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
  (stream, input_path, finalized, end_pos)

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
    List.map (fun (input_path, finalized) ->
      match member input_path start_positions |> to_int_option with
      | None -> (input_path, finalized, 0)
      | Some i -> (input_path, finalized, i)
    ) from_inputs
  in
  (* Merge from the start positions and print the result *)
  let input_streams = List.mapi mergeable_stream positioned_inputs in
  (* It's not safe to merge beyond the timestamp of the
     earliest discarded entry from any of the input streams *)
  let max_ts = ref (Nldate.create (float_of_int max_int)) in
  let truncated_streams =
    List.map (fun (stream, input_path, finalized, end_pos) ->
      (* If this file is finalized (no more data will be appended),
         we shouldn't truncate the stream *)
      if finalized then (stream, input_path, end_pos) else
      (* Otherwise, drop the last entry from the stream,
         and update end_pos accordingly *)
      let truncated =
        Stream.from (fun _ ->
          match Stream.npeek 2 stream with
          | [] -> None
          | [_] ->
              let ((ts, _), last) = Stream.next stream in
              end_pos := !end_pos - String.length last - 1;
              max_ts := min ts !max_ts;
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
  Stream.iter (fun ((ts, fileno), line) ->
    if (ts >= !max_ts) then
      let (_, _, end_pos) = List.nth truncated_streams fileno in
      end_pos := !end_pos - String.length line - 1
    else
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
  (* An input file foo.log.1 is finalized if the input file foo.log.2
     also exists in the log folder. We need to check this because the
     last entry of a non-finalized file limits how far we can merge. *)
  let finalized_inputs =
    List.map (fun filename ->
      let last_dot = String.rindex filename '.' + 1 in
      let suffix =
        String.(sub filename last_dot (length filename - last_dot))
      in
      let next_version = string_of_int (int_of_string suffix + 1) in
      let finalized =
        List.exists (fun other_file ->
          BatString.ends_with other_file ("." ^ next_version)
        ) inputs
      in
      (filename, finalized)
    ) inputs
  in
  let output =
    BatFile.open_out
      ~mode:[`create; `append]
      ~perm:(BatFile.unix_perm 0o666)
      log_merge_output
  in
  append_new_entries
    ~from_inputs:finalized_inputs
    ~at_positions:positions_file
    ~to_output:output;
  BatIO.close_out output

module Test = struct

  let foo_data_1 = "\
[2015-01-23T00:00:01.000-08:00] To be, or not to be, that is the question—
[2015-01-23T00:00:02.000-08:00] To die, to sleep—
No more; and by a sleep, to say we end
The Heart-ache, and the thousand Natural shocks
That Flesh is heir to?\n"

  let foo_data_2 = "\
[2015-01-23T00:04:01.000-08:00] To die, to sleep,
To sleep, perchance to Dream;
[2015-01-23T00:05:00.000-08:00] For who would bear the Whips and Scorns of time,
[2015-01-23T00:06:00.000-08:00] The Oppressor's wrong, the proud man's Contumely,\n"

  let foo_data_3 = "\
[2015-01-23T00:09:00.000-08:00] Who would these Fardels bear,
To grunt and sweat under a weary life,
But that the dread of something after death,
The undiscovered Country, from whose bourn
No Traveller returns, Puzzles the will,
And makes us rather bear those ills we have,
Than fly to others that we know not of.
[2015-01-23T00:20:00.000-08:00] And thus the Native hue of Resolution
Is sicklied o'er, with the pale cast of Thought,
[2015-01-23T10:00:00.000-08:00] THIS ENTRY SHOULD NOT BE MERGED\n"

  let bar_data_1 = "\
[2015-01-23T00:03:00.000-08:00] 'Tis a consummation
Devoutly to be wished.\n"

  let bar_data_2 = "\
[2015-01-23T00:04:03.000-08:00] There's the respect
That makes Calamity of so long life:
[2015-01-23T00:08:00.000-08:00] The insolence of Office, and the Spurns
That patient merit of the unworthy takes,\n"

  let bar_data_3 = "\
When he himself might his Quietus make
With a bare Bodkin?
[2015-01-23T00:10:00.000-08:00] Thus Conscience does make Cowards of us all,
[2015-01-23T00:30:00.000-08:00] And enterprises of great pitch and moment,
[2015-01-23T00:40:00.000-08:00] With this regard their Currents turn awry,
[2015-01-23T10:00:00.000-08:00] THIS ENTRY SHOULD NOT BE MERGED\n"

  let baz_data_1 = "\
[2015-01-23T00:00:01.200-08:00] Whether 'tis Nobler in the mind to suffer
The Slings and Arrows of outrageous Fortune,
Or to take Arms against a Sea of troubles,\n"

  let baz_data_2 = "\
And by opposing, end them?
[2015-01-23T00:04:02.000-08:00] Aye, there's the rub,
For in that sleep of death, what dreams may come,
When we have shuffled off this mortal coil,
Must give us pause.
[2015-01-23T00:07:00.000-08:00] The pangs of despised Love, the Law’s delay,\n"

  let baz_data_3 = "\
[2015-01-23T00:50:00.000-08:00] And lose the name of Action.
[2015-01-23T10:00:00.000-08:00] THIS ENTRY SHOULD NOT BE MERGED\n"

  let expected = "\
[2015-01-23T00:00:01.000-08:00] To be, or not to be, that is the question—
[2015-01-23T00:00:01.200-08:00] Whether 'tis Nobler in the mind to suffer
The Slings and Arrows of outrageous Fortune,
Or to take Arms against a Sea of troubles,
And by opposing, end them?
[2015-01-23T00:00:02.000-08:00] To die, to sleep—
No more; and by a sleep, to say we end
The Heart-ache, and the thousand Natural shocks
That Flesh is heir to?
[2015-01-23T00:03:00.000-08:00] 'Tis a consummation
Devoutly to be wished.
[2015-01-23T00:04:01.000-08:00] To die, to sleep,
To sleep, perchance to Dream;
[2015-01-23T00:04:02.000-08:00] Aye, there's the rub,
For in that sleep of death, what dreams may come,
When we have shuffled off this mortal coil,
Must give us pause.
[2015-01-23T00:04:03.000-08:00] There's the respect
That makes Calamity of so long life:
[2015-01-23T00:05:00.000-08:00] For who would bear the Whips and Scorns of time,
[2015-01-23T00:06:00.000-08:00] The Oppressor's wrong, the proud man's Contumely,
[2015-01-23T00:07:00.000-08:00] The pangs of despised Love, the Law’s delay,
[2015-01-23T00:08:00.000-08:00] The insolence of Office, and the Spurns
That patient merit of the unworthy takes,
When he himself might his Quietus make
With a bare Bodkin?
[2015-01-23T00:09:00.000-08:00] Who would these Fardels bear,
To grunt and sweat under a weary life,
But that the dread of something after death,
The undiscovered Country, from whose bourn
No Traveller returns, Puzzles the will,
And makes us rather bear those ills we have,
Than fly to others that we know not of.
[2015-01-23T00:10:00.000-08:00] Thus Conscience does make Cowards of us all,
[2015-01-23T00:20:00.000-08:00] And thus the Native hue of Resolution
Is sicklied o'er, with the pale cast of Thought,
[2015-01-23T00:30:00.000-08:00] And enterprises of great pitch and moment,
[2015-01-23T00:40:00.000-08:00] With this regard their Currents turn awry,
[2015-01-23T00:50:00.000-08:00] And lose the name of Action.\n"

  let not_finalized inputs = List.map (fun x -> (x, false)) inputs

  let test_log_merge () =
    (* Create the temporary files, write some sample log data *)
    let foo_path = "/tmp/foo.test.log" in
    let bar_path = "/tmp/bar.test.log" in
    let baz_path = "/tmp/baz.test.log" in
    let foo_out = open_out foo_path in
    let bar_out = open_out bar_path in
    let baz_out = open_out baz_path in
    output_string foo_out foo_data_1; flush foo_out;
    output_string bar_out bar_data_1; flush bar_out;
    output_string baz_out baz_data_1; flush baz_out;

    (* Merge the first round of log data *)
    let positions_path = "/tmp/positions.json" in
    (try Unix.unlink positions_path with Unix.Unix_error _ -> ()); (* rm *)
    let string_buffer = BatIO.output_string () in
    append_new_entries
      ~from_inputs:(not_finalized [foo_path; bar_path; baz_path])
      ~at_positions:positions_path
      ~to_output:string_buffer;

    (* Append some more log data to the temp files *)
    output_string foo_out foo_data_2; flush foo_out;
    output_string bar_out bar_data_2; flush bar_out;
    output_string baz_out baz_data_2; flush baz_out;
    append_new_entries
      ~from_inputs:(not_finalized [foo_path; bar_path; baz_path])
      ~at_positions:positions_path
      ~to_output:string_buffer;

    output_string foo_out foo_data_3; close_out foo_out;
    output_string bar_out bar_data_3; close_out bar_out;
    output_string baz_out baz_data_3; close_out baz_out;
    append_new_entries
      ~from_inputs:(not_finalized [foo_path; bar_path; baz_path])
      ~at_positions:positions_path
      ~to_output:string_buffer;

    (* The result should be sorted and missing the last line of each file *)
    let result = BatIO.close_out string_buffer in
    result = expected

  let tests = [("test_log_merge", test_log_merge)]

  let check_sorted file =
    let input = open_in file in
    let rec read prev_ts =
      match next_line input with
      | None -> true
      | Some line ->
          (match parse_timestamp line with
          | None -> read prev_ts
          | Some ts -> if ts >= prev_ts then read ts else false
          )
    in
    read (Nldate.create 0.)

end

let tests = Test.tests
