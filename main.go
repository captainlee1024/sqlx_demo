// Package main provides ...
package main

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

// 连接数据库
var db *sqlx.DB

func initMySQL() (err error) {
	dsn := "root:644315@tcp(127.0.0.1:3306)/go_test?charset=utf8mb4&parseTime=True"

	// 这里也可以使用MustConnect()，该函数不返回错误，连接不成功就panic
	// Go很多第三方库中带有Must开头的函数，都是不返回错误的，出错就内部panic
	// 这里的Connect里面就是调用了标准库里的Open和Ping
	db, err = sqlx.Connect("mysql", dsn)
	if err != nil {
		fmt.Printf("connect MySQL fialed, err:%v\n", err)
		return
	}

	db.SetMaxOpenConns(20) // 设置最大连接数
	db.SetMaxIdleConns(10) // 设置最大空闲连接数
	return
}

// 查询

type sqlxUser struct {
	Id   int    `db:"id"`
	Age  int    `db:"age"`
	Name string `db:"name"`
}

func (u sqlxUser) Value() (driver.Value, error) {
	return []interface{}{u.Name, u.Age}, nil
}

// 单行查询
func queryRowDemo() {
	sqlStr := `select id, name, age from mysql_demo_user where id = ?`
	var u sqlxUser
	err := db.Get(&u, sqlStr, 21)
	if err != nil {
		fmt.Printf("get failed, err:%v\n", err)
		return
	}
	fmt.Printf("id:%d name:%s age:%d\n", u.Id, u.Name, u.Age)
}

// 多行查询
func queryMultiRowDemo() {
	sqlStr := `select id, name, age from mysql_demo_user where id > ?`
	var users []sqlxUser
	err := db.Select(&users, sqlStr, 0)
	if err != nil {
		fmt.Printf("select fialed, err:%v\n", err)
		return
	}
	fmt.Printf("users:%#v\n", users)
}

// 插入数据
func insertRowDemo() {
	sqlStr := `insert into mysql_demo_user(name, age) values(?,?)`
	ret, err := db.Exec(sqlStr, "沙河小王子", 19)
	if err != nil {
		fmt.Printf("exec fialed, err:%v\n", err)
		return
	}
	var theId int64
	theId, err = ret.LastInsertId()
	if err != nil {
		fmt.Printf("get the id fialed, err:%d\n", err)
		return
	}
	fmt.Printf("the id:%d\n", theId)
}

// 更新数据
func updateRowDemo() {
	sqlStr := `update mysql_demo_user set age = ? where id = ?`
	ret, err := db.Exec(sqlStr, 29, 26)
	if err != nil {
		fmt.Printf("exec fialed, err:%v\n", err)
		return
	}
	var affRow int64
	affRow, err = ret.RowsAffected()
	if err != nil {
		fmt.Printf("get RowsAffected fialed, err:%v\n", err)
		return
	}
	fmt.Printf("update success, affected rows:%d\n", affRow)
}

// 删除数据
func deleteRowDemo() {
	sqlStr := `delete from mysql_demo_user where id = ?`
	ret, err := db.Exec(sqlStr, 26)
	if err != nil {
		fmt.Printf("exec failed:%v\n", err)
		return
	}
	var affRow int64
	affRow, err = ret.RowsAffected()
	if err != nil {
		fmt.Printf("get RowsAffected failed, err:%v\n", err)
		return
	}
	fmt.Printf("delete success, RowsAffected:%d\n", affRow)
}

// NamedExec
func insertUserDemo() (err error) {
	sqlStr := `insert into mysql_demo_user(name, age) values(:name, :age)`
	ret, err := db.NamedExec(sqlStr,
		map[string]interface{}{
			"name": "沙河小王子",
			"age":  28,
		})
	if err != nil {
		fmt.Printf("nameexec fialed, err:%v\n", err)
		return
	}
	var theId int64
	theId, err = ret.LastInsertId()
	if err != nil {
		fmt.Printf("get LastInsertId fialed, err:%v\n", err)
		return
	}
	fmt.Printf("nameexec insert success, theId:%d\n", theId)
	return
}

// NamedQuery
func namedQuery() {
	sqlStr := `select * from mysql_demo_user where name = :name`

	// 使用map做命名查询
	rows, err := db.NamedQuery(sqlStr, map[string]interface{}{"name": "沙河娜扎"})
	if err != nil {
		fmt.Printf("db.NamedQuery fialed, err:%v\n", err)
		return
	}
	// ！！！在　'判空之后'　一定要加一个延迟的释放连接来释放资源
	defer rows.Close()
	for rows.Next() {
		var u sqlxUser
		err := rows.StructScan(&u)
		if err != nil {
			fmt.Printf("scan failed, err:%v\n", err)
			return
		}
		fmt.Printf("user:%#v\n", u)
	}

	// 使用结构体命名查询，根据结构体字段的db tag 进行映射
	u := sqlxUser{
		Name: "沙河小王子",
	}
	rows, err = db.NamedQuery(sqlStr, u)
	if err != nil {
		fmt.Printf("db.NamedQuery failed, err:%v\n", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var u sqlxUser
		err := rows.StructScan(&u)
		if err != nil {
			fmt.Printf("scanf fialed, err:%v\n", err)
			return
		}
		fmt.Printf("user:%#v\n", u)
	}
}

// 事务操作
func transactionDemo2() (err error) {
	tx, err := db.Beginx()
	if err != nil {
		fmt.Printf("begin failed, err:%v\n", err)
		return err
	}

	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p) // recover-throw panic after Rollback
		} else if err != nil {
			fmt.Printf("rollback, err:%v\n", err)
			tx.Rollback() // err is non-nil; don't change it
		} else {
			err = tx.Commit() // err is nil, if COmmit returns error update err
			fmt.Printf("commit\n")
		}
	}()

	sqlStr1 := `update mysql_demo_user set age = 2 where id = ?`
	var ret1 sql.Result
	ret1, err = tx.Exec(sqlStr1, 21)
	if err != nil {
		fmt.Printf("exec1 fialed, err:%v\n", err)
		return err
	}
	//var affRow1 int64
	var n int64
	n, err = ret1.RowsAffected()
	if err != nil {
		fmt.Printf("get RowsAffected failed, err:%v\n", err)
		return err
	}
	if n != 1 {
		return errors.New("RowsAffected != 1, exec1 failed.")
	}

	sqlStr2 := `update mysql_demo_user set age = 2 where id = ?`
	//var ret2 sql.Result
	ret1, err = tx.Exec(sqlStr2, 28)
	if err != nil {
		fmt.Printf("exec2 failed, err:%v\n", err)
		return err
	}
	//var affRow2 int64
	n, err = ret1.RowsAffected()
	if err != nil {
		fmt.Printf("get RowsAffected failed, err:%v\n", err)
		return err
	}
	if n != 1 {
		return errors.New("RowsAffected != 1, exec2 failed.")
	}
	return err
}

// sqlx.In
// 批量插入

// 自己拼接语句实现批量插入
// BatchInsertUsers　自行构造批量插入语句 有多少个User需要插入就拼接多少个(?,?)
func BatchInsertUsers(users []*sqlxUser) error {
	// 存放(?,?)的slice
	valueStrings := make([]string, 0, len(users))
	// 存放values的slice
	valueArgs := make([]interface{}, 0, len(users)*2)
	// 遍历users准备相关数据
	for _, u := range users {
		// 此处占位符要与插入的值的个数对应
		valueStrings = append(valueStrings, "(?,?)")
		valueArgs = append(valueArgs, u.Name)
		valueArgs = append(valueArgs, u.Age)
	}
	//自行拼接要执行的具体语句
	stmt := fmt.Sprintf("insert into mysql_demo_user (name, age) values%s",
		strings.Join(valueStrings, ","))
	_, err := db.Exec(stmt, valueArgs...)
	if err != nil {
		fmt.Printf("exec failed, err:%v\n", err)
	}
	fmt.Printf("batchInsert success\n")
	return err
}

// 使用sqlx.In实现批量插入
// BatchInsertUsers2 使用sqlx.In帮我们拼接语句和参数，注意传入的参数是[]interface{}
func BatchInsertUsers2(users []interface{}) error {
	query, args, _ := sqlx.In(
		"insert into mysql_demo_user (name, age) values (?), (?), (?)",
		users..., // 如果实现了driver.Valuer, sqlx.In会通过调用Value()来展开它
	)
	fmt.Println(query) // 查看生成的querystring
	fmt.Println(args)  // 查看生成的args
	_, err := db.Exec(query, args...)
	if err != nil {
		fmt.Printf("exec fialed, err:%v\n", err)
		return err
	}
	fmt.Printf("batchInsert2 success\n")
	return err
}

// 使用NameExec实现批量插入
// BatchInsertUsers3 使用NameExec实现批量插入
// 注：(sqlx当前版本v1.2.0)该功能已经有人推了#285PR，但是作者还没有发release
// 想要使用该方法实现批量插入需要暂时使用master分支
// 执行该命令下载并使用msater分支代码　go get github.com/jmoiron/sqlx@master
func BatchInsertUsers3(users []*sqlxUser) error {
	_, err := db.NamedExec("insert into mysql_demo_user (:name, :age)", users)
	return err
}

// 批量查询

// 查询id在给定id集合中的数据
// QueryByIds 根据给定ID查询
func QueryByIds(ids []int) (users []sqlxUser, err error) {
	// 动态填充id
	query, args, err := sqlx.In("select name, age from mysql_demo_user where id in (?)", ids)
	if err != nil {
		fmt.Printf("select failed, err:%b\n", err)
		return
	}
	// sqlx.In返回带`?`bindvar 的查询语句，我们使用Rebind()重新绑定它
	query = db.Rebind(query)

	err = db.Select(&users, query, args...)
	return
}

// QueryAndOrderByIds 按照指定id查询，并维护原有的顺序
// 两种方法：1.使用代码给返回的查询数据进行排序　2. 让MySQL进行排序，返回维护的的数据顺序
// 这里采用第二种方法，使用Mysql的FIND_IN_SET函数
func QueryAndOrderByIds(ids []int) (users []sqlxUser, err error) {
	// 动态填充id
	strIds := make([]string, 0, len(ids))
	for _, id := range ids {
		strIds = append(strIds, fmt.Sprintf("%d", id))
	}
	query, args, err := sqlx.In(
		`select name, age from mysql_demo_user where id in (?) 
		order by FIND_IN_SET(id,?)`, ids, strings.Join(strIds, ","))
	if err != nil {
		fmt.Printf("sqlx.in failed, err:%v\n", err)
		return
	}

	// sqlx.in 返回带`?`bindvar的查询语句，我们使用Rebind()重新板顶它
	query = db.Rebind(query)

	err = db.Select(&users, query, args...)
	return
}

func main() {
	err := initMySQL()
	if err != nil {
		fmt.Printf("init MySQL fialed, err:%v\n", err)
		return
	}
	fmt.Printf("init MySQL success!\n")

	// 查询
	//queryRowDemo()
	//queryMultiRowDemo()
	//insertRowDemo()
	//updateRowDemo()
	//deleteRowDemo()
	//insertUserDemo()
	//namedQuery()

	// 事务
	//transactionDemo2()

	// 批量插入
	//u1 := sqlxUser{Name: "xx", Age: 20}
	//u2 := sqlxUser{Name: "xxx", Age: 20}
	//u3 := sqlxUser{Name: "xxxx", Age: 20}
	//users := []*sqlxUser{&u1, &u2, &u3}
	//BatchInsertUsers(users)

	//u1 := sqlxUser{Name: "sqlxIn1", Age: 21}
	//u2 := sqlxUser{Name: "sqlxIn2", Age: 21}
	//u3 := sqlxUser{Name: "sqlxIn3", Age: 21}
	//users := []interface{}{u1, u2, u3}
	//BatchInsertUsers2(users)

	// 批量查询
	users, err := QueryByIds([]int{21, 30, 22, 32, 23, 33})
	if err != nil {
		fmt.Printf("QueryByIds failed, err:%v\n", err)
		return
	}
	for _, user := range users {
		fmt.Printf("user:%#v\n", user)
	}

	users, err = QueryAndOrderByIds([]int{21, 30, 22, 32, 23, 33})
	if err != nil {
		fmt.Printf("QueryAndOrderByIds failed, err:%v\n", err)
		return
	}
	for _, user := range users {
		fmt.Printf("user:%#v\n", user)
	}

}
