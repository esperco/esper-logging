open Printf
open Unix

type level = [ `Debug | `Info | `Warning | `Error | `Critical ]

let min_level = ref `Info
let level = min_level
let service = ref ""
let log_folder = "/var/log/badger"

let string_of_level = function
  | `Debug -> "debug"
  | `Info -> "info"
  | `Warning -> "warning"
  | `Error -> "error"
  | `Critical -> "critical"

let date () =
  let t = Unix.gettimeofday () in
  (* Pacific time always *)
  Nldate.mk_internet_date ~localzone:false ~zone:(-480) ~digits:3 t

let int = function
  | `Debug -> 0
  | `Info -> 1
  | `Warning -> 2
  | `Error -> 3
  | `Critical -> 4

let sensitive_fields_re = Pcre.regexp ~flags:[`CASELESS] "(?<=password|facebook_token).{0,20}"

let hide_sensitive_fields s =
  Pcre.substitute
    ~rex: sensitive_fields_re
    ~subst: (fun s -> String.make (String.length s) '*')
    s

let () =
  assert
    (hide_sensitive_fields
       ";alksjdf;asdpasswordfja;slkdfa;sdkfjaspasSwordl;kfasdf"
     = ";alksjdf;asdpassword********************sSword********")

let log level s =
  if int level >= int !min_level then
    eprintf "[%s] [%s] [%s] %s\n%!"
      (date ()) !service (string_of_level level) (hide_sensitive_fields s)

let logf level msgf =
  Printf.kprintf (log level) msgf

let debug f =
  if !min_level = `Debug then
    log `Debug (f ())

let string_of_exn e =
  Util_exn.string_of_exn e

let () =
  logf `Info "Setting Lwt.async_exception_hook";
  Lwt.async_exception_hook :=
    (fun e ->
      logf `Error "Async exception caught: %s" (string_of_exn e)
    )

let tests = [
  "hide_sensitive_fields", (fun () ->
    hide_sensitive_fields
      ";alksjdf;asdpasswordfja;slkdfa;sdkfjaspasSwordl;kfasdf"
    = ";alksjdf;asdpassword********************sSword********"
  )
]
