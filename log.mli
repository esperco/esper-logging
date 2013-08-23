type level = [ `Debug | `Info | `Warning | `Error | `Critical ]

val log_folder : string
  (** Folder to store logs files *)

val level : level ref
  (** Log level. Initial value: [`Info] *)

val service : string ref
  (** Name of the current service. Initial value: [""] *)

val logf : level -> ('a, unit, string, unit) format4 -> 'a
  (** Like printf but with a log level before the format string *)

val debug : (unit -> string) -> unit
  (** Log something that's a bit expensive to compute,
      at the `Debug level. *)

val string_of_exn : exn -> string
  (** String representation of an exception + stack backtrace *)

val tests : (string * (unit -> bool)) list
