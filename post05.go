package post05

import (
	"database/sql"
	"fmt"
	"strings"
)

/**
Typically we do four operations
Create user
Delete user
Update user
List users
Apart from the above, we also have
Functions to open connection sql, Run query to find the userId
*/

type Userdata struct {
	ID          int
	Username    string
	Name        string
	Surname     string
	Description string
}

// Connection details
var (
	Hostname = ""
	Port     = 2345
	Username = ""
	Password = ""
	Database = ""
)

// Open the sql connection
func openConnection() (*sql.DB, error) {
	// Connection string
	conn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", Hostname, Port, Username, Password, Database)
	// Open database
	db, err := sql.Open("postgres", conn)
	if err != nil {
		return nil, err
	}
	return db, nil
}

// Function returns UserId of username if it exists -1 if user is not existing
func exists(username string) int {
	username = strings.ToLower(username)
	db, err := openConnection()
	if err != nil {
		fmt.Println("Error ", err)
		return -1
	}
	defer func(db *sql.DB) {
		err = db.Close()
		if err != nil {
			fmt.Println("DB connection error ", err)
		}
	}(db)
	userId := -1
	statement := fmt.Sprintf(`SELECT "id" from "users" where username = '%s'`, username)
	rows, err := db.Query(statement)
	for rows.Next() {
		var id int
		err = rows.Scan(&id)
		if err != nil {
			fmt.Println("Scan error ", err)
			return -1
		}
		userId = id
	}
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			fmt.Println("Row error ", err)
		}
	}(rows)
	return userId
}

// AddUser Creating the user i.e., adds new user to db returns -1 if error, or returns new user id
func AddUser(d Userdata) int {
	d.Username = strings.ToLower(d.Username)
	db, err := openConnection()
	if err != nil {
		fmt.Println("DB Error ", err)
		return -1
	}
	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			fmt.Println("Error ", err)
		}
	}(db)
	userId := exists(d.Username)
	if userId != -1 {
		fmt.Println("User id exists ", userId)
		return -1
	}
	insertStatement := `INSERT INTO "users" ("username") values ($1)`
	_, err = db.Exec(insertStatement, d.Username)
	if err != nil {
		fmt.Println(err)
		return -1
	}
	userId = exists(d.Username)
	if userId == -1 {
		return userId
	}
	insertStatement = `insert into "userdata" ("userid", "name", "surname", "description") values ($1, $2, $3, $4)`
	_, err = db.Exec(insertStatement, userId, d.Name, d.Surname, d.Description)
	if err != nil {
		fmt.Println("error", err)
		return -1
	}
	return userId
}

// DeleteUser deleting the user from the db
func DeleteUser(id int) error {
	db, err := openConnection()
	if err != nil {
		fmt.Println("Db error ", err)
		return err
	}
	defer func(db *sql.DB) {
		err = db.Close()
		if err != nil {
			fmt.Println("Db closing error ", err)
		}
	}(db)
	// Does the ID exists???
	statement := fmt.Sprintf(`SELECT "username" FROM "users" where id = %d`, id)
	rows, err := db.Query(statement)
	if err != nil {
		fmt.Println("Error ", err)
		return err
	}
	var username string
	for rows.Next() {
		err = rows.Scan(&username)
		if err != nil {
			return err
		}
	}
	defer func(rows *sql.Rows) {
		err = rows.Close()
		if err != nil {
			fmt.Println("Error ", err)
		}
	}(rows)
	if exists(username) != id {
		return fmt.Errorf("User with ID %d does not exists ", id)
	}
	// Delete from user and userdata table
	deleteStatement := `delete from "userdata" where userid=($1)`
	_, err = db.Exec(deleteStatement, id)
	if err != nil {
		return err
	}
	deleteStatement = `delete from "users" where id=$1`
	_, err = db.Exec(deleteStatement, id)
	if err != nil {
		return err
	}
	return nil
}

// ListUsers Listing the users
func ListUsers() ([]Userdata, error) {
	Data := []Userdata{}
	db, err := openConnection()
	if err != nil {
		fmt.Println("Error ", err)
		return nil, err
	}
	defer func(db *sql.DB) {
		err = db.Close()
		if err != nil {
			fmt.Println("DB error ", err)
		}
	}(db)
	statement := `SELECT "id", "username", "name", "surname", "description" FROM "users", "userdata" 
					WHERE users.id == userdata.userid`
	rows, err := db.Query(statement)
	for rows.Next() {
		var id int
		var username string
		var name string
		var surname string
		var description string
		err = rows.Scan(&id, &username, &name, &surname, &description)

		temp := Userdata{
			ID:          id,
			Username:    username,
			Surname:     surname,
			Name:        name,
			Description: description,
		}
		Data = append(Data, temp)
		if err != nil {
			fmt.Println("Rows error", err)
			return Data, err
		}
	}
	defer func(rows *sql.Rows) {
		err = rows.Close()
		if err != nil {
			fmt.Println(err)
		}
	}(rows)
	return Data, nil
}
