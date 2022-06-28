package main

import (
	"context"
	"fmt"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"

	"github.com/xbitgo/components/filterx"
)

type Book struct {
	Name string
}

func main() {
	db, err := gorm.Open(mysql.Open("root:@tcp(localhost:3306)/novel?charset=utf8mb4&interpolateParams=true&parseTime=true&loc=Local"), &gorm.Config{})
	if err != nil {
		panic(err)
	}
	session := db.Session(&gorm.Session{NewDB: true, Context: context.Background()})
	session = session.Debug()
	session = session.Table("book")
	querys := filterx.FilteringList{{
		Field:    "name",
		Operator: filterx.Like,
		Value:    "xxw",
	}, {
		Operator: filterx.OR,
		Children: []*filterx.Filtering{
			{
				Field:    "id",
				Operator: filterx.EQ,
				Value:    "1",
			},
			{
				Field:    "id",
				Operator: filterx.EQ,
				Value:    "100",
			},
			{
				Field:    "id",
				Operator: filterx.FindInSet,
				Value:    []int16{1, 3},
			},
			{
				Field:    "id",
				Operator: filterx.IN,
				Value:    []string{"1", "3", "10"},
			},
		},
	}, {
		Field:    "id",
		Operator: filterx.FindInSet,
		Value:    4,
	}, {
		Field:    "id",
		Operator: filterx.IN,
		Value:    []string{"1", "3", "10"},
	}}

	fmt.Println(querys.ToSqlWhere())
	session, err = querys.GormOption(session)
	if err != nil {
		panic(err)
	}
	list := make([]Book, 0)
	rs := session.Find(&list)
	fmt.Println(rs.Error)

}
