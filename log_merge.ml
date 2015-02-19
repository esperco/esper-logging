(*
  We process each log file line by line, scanning the lines into memory.
  They're kept sorted by timestamp in a Map, since we want the output merged
  in timestamp order.

  If multiple lines share a timestamp, we output all lines from the first file
  in line number order, then all lines from the second file, etc.

  A log line is expected to start with its timestamp in square brackets. If it
  does not, as occurs with pretty-printed JSON and exception stack traces,
  then we assume its timestamp is the same as the last one seen.
*)


open Batteries

module LogLines = Map.Make(struct
  type t = Nldate.t
  let compare = Pervasives.compare
end)


let parse_timestamp line =
  try
    let subs = Pcre.exec ~pat:"\\[([^]]*)\\]" line in
    Some (Nldate.parse (Pcre.get_substring subs 1))
  with
  | Not_found -> None
  | Invalid_argument "Netdate.parse" -> failwith ("Corrupt timestamp: " ^ line)

let add_or_replace tbl filename line =
  try
    let lines_from_same_file = Hashtbl.find tbl filename in
    Hashtbl.replace tbl filename (line :: lines_from_same_file)
  with Not_found ->
    Hashtbl.add tbl filename [line]

let create_new filename line =
  let lines_by_file = Hashtbl.create 8 in
  Hashtbl.add lines_by_file filename [line];
  lines_by_file

let parse log filename input =
  let read log last_known_ts =
    Enum.fold (fun (log, last_known_ts) line ->
      (match parse_timestamp line with
      | None ->
          let prevs = LogLines.find last_known_ts log in
          add_or_replace prevs filename line;
          (log, last_known_ts)
      | Some ts ->
          (try
            let lines_with_same_ts = LogLines.find ts log in
            add_or_replace lines_with_same_ts filename line;
            (log, ts)
          with Not_found ->
            let lines_by_file = create_new filename line in
            (LogLines.add ts lines_by_file log, ts)
          )
      )
    ) (log, last_known_ts) input
  in
  match Enum.get input with
  | None -> log
  | Some first_line ->
      (match parse_timestamp first_line with
      | None ->
          failwith ("Log file " ^ filename ^ " must start with a timestamp")
      | Some first_ts ->
          let lines_by_file = create_new filename first_line in
          Hashtbl.add lines_by_file filename [first_line];
          fst (read (LogLines.add first_ts lines_by_file log) first_ts)
      )

let print log filenames output =
  LogLines.iter (fun _ lines_by_file ->
    List.iter (fun filename ->
      try
        let lines = Hashtbl.find lines_by_file filename in
        List.iter (fun line ->
          BatIO.nwrite output (line ^ "\n")
        ) (List.rev lines)
      with Not_found -> ()
    ) filenames
  ) log;
  BatIO.close_out output


module Test = struct
  let test_log_merge () =
    let foo_data = "\
[2015-02-19T10:29:54.482-08:00] foo line 1
[2015-02-19T10:30:28.137-08:00] foo line 2, continued below
continuation of foo line 2
[2015-02-19T10:31:27.003-08:00] foo line 3
[2015-02-19T10:31:27.003-08:00] foo line 4
[2015-02-19T10:33:42.921-08:00] foo line 5
[2015-02-19T10:45:54.162-08:00] foo line 6" in
    let bar_data = "\
[2015-02-19T10:29:54.528-08:00] bar line 1
[2015-02-19T10:30:28.137-08:00] bar line 2
[2015-02-19T10:32:03.423-08:00] bar line 3, continued below
continuation of bar line 3 part 1/2
continuation of bar line 3 part 2/2
[2015-02-19T10:34:12.456-08:00] bar line 4" in
    let foo = IO.lines_of (IO.input_string foo_data) in
    let bar = IO.lines_of (IO.input_string bar_data) in
    let files = [("foo", foo); ("bar", bar)] in
    let log =
      List.fold_left (fun log (filename, input) ->
        parse log filename input
      ) LogLines.empty files
    in
    let output = print log (List.map fst files) (BatIO.output_string ()) in
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
[2015-02-19T10:33:42.921-08:00] foo line 5
[2015-02-19T10:34:12.456-08:00] bar line 4
[2015-02-19T10:45:54.162-08:00] foo line 6\n" in
    output = expected

  let tests = [("test_log_merge", test_log_merge)]
end

let tests = Test.tests
