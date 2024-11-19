package main

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type PaymentType string
type PaymentCurrency string

const (
	Default PaymentType = "Default"
)

const (
	Idr PaymentCurrency = "Idr"
	Us  PaymentCurrency = "Us"
)

const DB_CONTEXT = "databaseConn"
const DEFAULT_USERNAME = "mock_user"
const DEFAULT_USER_BALANCE = 10000

type PaymentMethod struct {
	UserId   string          `db:"user_id"`
	Balance  float64         `db:"balance"`
	Type     PaymentType     `db:"type"`
	Currency PaymentCurrency `db:"currency"`
}

type User struct {
	Username string `db:"username"`
}

type TransactionLog struct {
	Tid               string          `db:"tid"`
	UserId            string          `db:"user_id"`
	PaymentMethodType PaymentType     `db:"payment_method_type"`
	Currency          PaymentCurrency `db:"currency"`
	CheckoutAmount    float64         `db:"checkout_amount"`
}

type DisburseRequest struct {
	UserId         string          `json:"user_id"`
	Currency       PaymentCurrency `json:"currency"`
	Type           PaymentType     `json:"type"`
	CheckoutAmount float64         `json:"checkout_amount"`
}

type DisburseResponse struct {
	Message string `json:"message"`
}

func recordTransaction(transactionLog TransactionLog, db *sqlx.DB) error {
	query := `
		INSERT INTO transaction_logs (tid, user_id, payment_method_id, checkout_amount)
		VALUES (:tid, :user_id, :payment_method_id, :checkout_amount)
	`

	result, err := db.NamedExec(query, transactionLog)

	if err != nil {
		return fmt.Errorf("[recordTransaction] %w", err)
	}
	affectedRows, err := result.RowsAffected()

	if err != nil {
		return fmt.Errorf("[recordTransaction] %w", err)
	}

	if affectedRows == 0 {
		return fmt.Errorf("[recordTransaction] %w", err)
	}

	return nil
}

func addPaymentMethod(paymentMethod PaymentMethod, db *sqlx.DB) error {
	query := `
		INSERT INTO payment_methods (user_id, balance, type, currency)
		VALUES (:user_id,  :balance, :type, :currency)
	`

	result, err := db.NamedExec(query, paymentMethod)

	if err != nil {
		return fmt.Errorf("[addPaymentMethod] %w", err)
	}
	affectedRows, err := result.RowsAffected()

	if err != nil {
		return fmt.Errorf("[addPaymentMethod] %w", err)
	}

	if affectedRows == 0 {
		return fmt.Errorf("[addPaymentMethod] %w", err)
	}

	return nil
}

func addUser(user User, db *sqlx.DB) error {

	query := `
		INSERT INTO users (username)
		VALUES (:username) 
	`

	result, err := db.NamedExec(query, user)

	if err != nil {
		return fmt.Errorf("[addUser] %w", err)
	}

	affectedRows, err := result.RowsAffected()

	if err != nil {
		return fmt.Errorf("[addUser] %w", err)
	}

	if affectedRows == 0 {
		return fmt.Errorf("[recrodTransaction] %w", err)
	}

	return nil
}

func getPaymentMethod(userId string, currency PaymentCurrency, paymentType PaymentType, db *sqlx.DB) (*PaymentMethod, error) {
	query := `
		SELECT user_id, currency, type, balance FROM payment_methods
		WHERE user_id = $1 AND currency = $2 and type = $3
	`

	row := db.QueryRow(query, userId, currency, paymentType)
	var paymentMethod PaymentMethod

	if err := row.Scan(&paymentMethod.UserId, &paymentMethod.Currency, &paymentMethod.Type, &paymentMethod.Balance); err != nil {
		return nil, fmt.Errorf("[getPaymentMethod] %w", err)
	}

	return &paymentMethod, nil
}

func DisburseUser(request DisburseRequest, ctx context.Context, db *sqlx.DB) error {

	tx, err := db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
	})

	if err != nil {
		return fmt.Errorf("[DisburseUser] %w", err)
	}

	findPaymentMethodQuery := `
		SELECT user_id, balance, type, currency from payment_methods
		WHERE user_id = $1 AND currency = $2 AND type = $3
	`

	findResult := tx.QueryRow(findPaymentMethodQuery, request.UserId, request.Currency, request.Type)
	var userPaymentMethod PaymentMethod

	err = findResult.Scan(
		&userPaymentMethod.UserId,
		&userPaymentMethod.Balance,
		&userPaymentMethod.Type,
		&userPaymentMethod.Currency,
	)

	if err != nil {
		tx.Rollback()
		return fmt.Errorf("[DisburseUser] %w", err)
	}

	if userPaymentMethod.Balance < request.CheckoutAmount {
		tx.Rollback()
		return fmt.Errorf("[DisburseUser] not enough balance, current user balance: %f, checkout balance: %f", userPaymentMethod.Balance, request.CheckoutAmount)
	}

	userPaymentMethod.Balance -= request.CheckoutAmount

	updateBalanceQuery := `
		UPDATE payment_methods
		SET balance = $1
		WHERE user_id = $2 AND type = $3 AND currency = $4
	`

	updateBalanceResult, err := tx.Exec(updateBalanceQuery, userPaymentMethod.Balance, userPaymentMethod.UserId, userPaymentMethod.Type, userPaymentMethod.Currency)

	if err != nil {
		tx.Rollback()
		return fmt.Errorf("[DisburseUser] %w", err)
	}

	if affected, err := updateBalanceResult.RowsAffected(); err != nil || affected == 0 {
		return fmt.Errorf("[DisburseUser] %w", err)
	}

	return tx.Commit()
}

func dropTable(db *sqlx.DB) error {
	query := `
		DROP TABLE IF EXISTS transaction_logs;
		DROP TABLE IF EXISTS payment_methods;
		DROP TABLE IF EXISTS users;
	`

	_, err := db.Exec(query)
	return err
}

func initData(db *sqlx.DB) error {

	schema := `
		CREATE TABLE users (
			username VARCHAR(32) PRIMARY KEY
		);

		CREATE TABLE transaction_logs (
			tid SERIAL PRIMARY KEY,
			user_id VARCHAR(32) NOT NULL REFERENCES users(username),
			payment_method_type VARCHAR(32) NOT NULL,
			currency VARCHAR(32) NOT NULL,
			checkout_amount decimal NOT NULL
		);

		CREATE TABLE payment_methods (
			user_id VARCHAR(32) NOT NULL REFERENCES users(username),
			type VARCHAR(32) NOT NULL,
			currency VARCHAR(32) NOT NULL,
			balance DECIMAL DEFAULT 1000,
			
			PRIMARY KEY(user_id, type, currency)
	 	);
	`

	_, err := db.Exec(schema)

	if err != nil {
		return err
	}

	user := User{Username: DEFAULT_USERNAME}

	if err = addUser(user, db); err != nil {
		return err
	}

	paymentMethod := PaymentMethod{
		UserId:   DEFAULT_USERNAME,
		Balance:  DEFAULT_USER_BALANCE,
		Type:     Default,
		Currency: Idr,
	}

	if err = addPaymentMethod(paymentMethod, db); err != nil {
		return err
	}

	return err
}

func ApiMiddleware(db *sqlx.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Set(DB_CONTEXT, db)
		c.Next()
	}
}

func disbuseHandler(ctx *gin.Context) {
	var req DisburseRequest

	if err := ctx.ShouldBindJSON(&req); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"message": err.Error()})
		return
	}

	db := ctx.MustGet(DB_CONTEXT).(*sqlx.DB)

	if err := DisburseUser(req, ctx, db); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"message": err.Error()})
		return
	}

	transactionLog := TransactionLog{
		UserId:            req.UserId,
		PaymentMethodType: req.Type,
		Currency:          req.Currency,
		CheckoutAmount:    req.CheckoutAmount,
	}

	if err := recordTransaction(transactionLog, db); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"message": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{"message": "Disburse succesfull"})
}

func main() {
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		"localhost", 5432, "user", "password", "db", "disable")

	db, err := sqlx.Connect("postgres", dsn)
	if err != nil {
		panic(err)
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)

	err = dropTable(db)

	if err != nil {
		panic(err)
	}

	err = initData(db)

	if err != nil {
		panic(err)
	}

	router := gin.Default()
	router.Use(ApiMiddleware(db))
	router.POST("/api/disburse", disbuseHandler)

	router.Run()
}
