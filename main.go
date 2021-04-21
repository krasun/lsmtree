package main

import (
	"fmt"
)

func main() {
	s, _ := Open("test.db")

	s.Put([]byte("test"), []byte("val1"))
	s.Put([]byte("test1"), []byte("val1"))
	s.Delete([]byte("test"))

	val, ok, _ := s.Get([]byte("test1"))
	fmt.Println(string(val))
	fmt.Println(ok)

	val2, ok, _ := s.Get([]byte("test"))
	fmt.Println(val2)
	fmt.Println(ok)
	fmt.Println(len(val2))
	fmt.Println(nil == val2)
}
