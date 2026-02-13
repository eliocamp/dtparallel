#' Run expression in parallel
#'
#' @param ... expression passed to future::future()
#'
#' @export
in_par <- function(...) {
  method <- parallel_method_get()
  method <- switch(method, future = async_future, mirai = async_mirai)
  method(...)
}


#' Set parallel backend
#'
#' @param method method to use.
#'
#' @export
#' @rdname method
parallel_method_get <- function() {
  getOption("async.dt.method", default = "future")
}

#' @export
#' @rdname method
parallel_method_set <- function(method = c("future", "mirai")) {
  old <- parallel_method_get()
  options(async.dt.method = method[1])
  return(invisible(old))
}

async_future <- function(...) {
  rlang::check_installed("future")
  tasks <- list(future::future(..., envir = parent.frame(2)))
  class(tasks) <- c("tasks_future", class(tasks[1]))
  tasks
}

async_mirai <- function(...) {
  rlang::check_installed("mirai")
  tasks <- list(mirai::mirai(..., parent.frame(2)))
  class(tasks) <- c("tasks_mirai", class(tasks[1]))
  tasks
}

#' Collect parallel execution
#'
#' @param x object
#'
#' @export
collect_par <- function(x) {
  UseMethod("collect_par")
}

#' @export
collect_par.default <- function(x) x

#' @export
collect_par.tasks_future <- function(x) {
  class(x) <- setdiff(class(x), "tasks_future")
  unlist(future::value(x))
}

#' @export
collect_par.tasks_mirai <- function(x) {
  x[.progress]
  # unlist(mirai::collect_mirai(x, options = ".progress"))
}

#' @export
collect_par.data.table <- function(x) {
  x[, names(.SD) := lapply(.SD, collect_par)][]
}

.datatable.aware = TRUE
