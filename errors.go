package simpledb

import "fmt"

// TODO add other custom errors, rather than strings, although now it is not really important
type NotFoundError struct {
	id  ID
	Err error
}

func (r *NotFoundError) Error() string {
	return fmt.Sprintf("item %d not found", r.id)
}

type DbGeneralError struct {
	err string
}

func (r *DbGeneralError) Error() string {
	return fmt.Sprintf("error: method %s", r.err)
}

type DbInternalError struct {
	oper string
	err  error
}

func (r *DbInternalError) Error() string {
	return fmt.Sprintf("internal error: %s :%W", r.oper, r.err)
}
