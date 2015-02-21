(*
   This merges multiple log files into one file, sorted by timestamp.
   It can start from a previous position, for periodic incremental merging.
*)


(* We'll use a (filename, entry list) Hashtbl.t LogEntries.t,
   where type filename = string and entry = string.
   So, a LogEntries.t maps timestamps to the ordered entries from each file
   which share that timestamp, and it keeps everything ordered by timestamp.
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
    let subs = Pcre.exec ~pat:"\\[([^]]*)\\]" line in
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
let create_ts_table filename line =
  let entries_by_file = Hashtbl.create 8 in
  Hashtbl.add entries_by_file filename [line];
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
            parse_until_end updated_log filename input first_ts
          in
          close_in input;
          log_with_parsed_input
      )

(* Print the entries from the LogEntries.t log in timestamp sorted order.
   When multiple files have entries with the same timestamp, print the entries
   in the order they appeared in their file, and process files in the order
   given by filenames.
*)
let print log filenames output =
  LogEntries.iter (fun _ lines_by_file ->
    List.iter (fun filename ->
      try
        let lines = Hashtbl.find lines_by_file filename in
        List.iter (fun line ->
          BatIO.nwrite output (line ^ "\n")
        ) (List.rev lines)
      with Not_found -> ()
    ) filenames
  ) log

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
    let start_inputs = [(foo_path, 0); (bar_path, 0)] in
    let start_log = LogEntries.empty in
    let (mid_log, mid_inputs) = merge start_log start_inputs in
    assert (mid_inputs = [(foo_path, 130); (bar_path, 86)]);

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
    let (end_log, end_inputs) = merge mid_log mid_inputs in
    assert (end_inputs = [(foo_path, 259); (bar_path, 218)]);

    (* The result should be sorted and missing the last line of each file *)
    let output = BatIO.output_string () in
    print end_log (List.map fst end_inputs) output;
    let result = BatIO.close_out output in
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
