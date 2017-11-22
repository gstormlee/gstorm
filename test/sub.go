package main

import(
   "reflect"
   "fmt"
)

type A struct {
}


func (self A)Run() {
   c := reflect.ValueOf(self)
   method := c.MethodByName("Test")
   println(method.IsValid())
}

type B struct {
   A
}

func (self B)Test(s string){
   println("b")
}
func(self B)Run() {
    fmt.Println("b run")
}

func main() {
   b := new(B)
   b.Run()
   b.Test("1")
}