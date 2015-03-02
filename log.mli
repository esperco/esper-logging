type level = [ `Debug | `Info | `Warning | `Error | `Critical ]

val log_folder : string
  (** Folder to store logs files *)

val level : level ref
  (** Log level. Initial value: [`Info] *)

val set_service : string -> unit
  (** Name of the current service.
      If defined, this is printed between square brackets
      on each new log line. *)

val logf : level -> ('a, unit, string, unit) format4 -> 'a
  (** Like printf but with a log level before the format string *)

val debug : (unit -> string) -> unit
  (** Log something that's a bit expensive to compute,
      at the `Debug level. *)

val string_of_exn : exn -> string
  (** String representation of an exception + stack backtrace *)

val with_request_id : (unit -> 'a) -> 'a
  (** where 'a can be an lwt thread, within which the random request ID
      is logged by each logf call. *)

val tests : (string * (unit -> bool)) list
