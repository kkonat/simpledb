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
