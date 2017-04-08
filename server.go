package main

import (
  "github.com/benzeneDB/benzeneDB"
  "github.com/julienschmidt/httprouter"
  "net/http"
  "github.com/google/glog"
  "github.com/pquerna/ffjson/ffjson"
)

func

func CreateTable(w http.ResponseWriter, r *http.Request, ps httprouter.Params){
/**
TODO:
接受一个类似于:
{
  table_name:*table_name*,
  keys:{
    包含: int,float,string,array,bool.
  }
}

之后创建一个Table
**/
}

func TableDetail(w http.ResponseWriter, r *http.Request, ps httprouter.Params){

}

func DataQeury(w http.ResponseWriter, r *http.Request, ps httprouter.Params){
/**
TODO:
查询以range为基本单位:
{
  start:timestamp,
  end:timestamp
  data:{
    特定段，可选.
  }
}
**/
}

func NewData(w http.ResponseWriter, r *http.Request, ps httprouter.Params){

}

func main(){
  router := httprouter.New()
  router.POST("/create", CreateTable)
  router.GET("/:table_name", TableDetail)
  router.GET("/:table_name/query",DataQeury)
  router.POST("/:table_name", NewData)

  log.Fatal(http.ListenAndServe(":8080", router))
}
