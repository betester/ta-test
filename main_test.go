package main

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/jmoiron/sqlx"
)

//TODO: test in here the following
// 0. basic usage (done)
// 1. do concurrent update on the price with the expected price
// 2. do floating point related issues  testing

func TestDisburseUser(t *testing.T) {

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
		t.Fatal(err)
	}

	err = initData(db)

	if err != nil {
		t.Fatal(err)
	}

	req := DisburseRequest{
		UserId:         DEFAULT_USERNAME,
		Currency:       Idr,
		Type:           Default,
		CheckoutAmount: 250,
	}

	ctx := context.Background()

	if err := DisburseUser(req, ctx, db); err != nil {
		t.Fatal(err)
	}

	paymentMethod, err := getPaymentMethod(DEFAULT_USERNAME, Idr, Default, db)

	if err != nil {
		t.Fatal(err)
	}

	if paymentMethod.Balance != DEFAULT_USER_BALANCE-250 {
		t.Fatalf("incorrect result %f", paymentMethod.Balance)
	}

}

func TestConcurrentTransaction(t *testing.T) {
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
		t.Fatal(err)
	}

	err = initData(db)

	if err != nil {
		t.Fatal(err)
	}

	checkoutAmount := 250

	ctx := context.Background()

	req := DisburseRequest{
		UserId:         DEFAULT_USERNAME,
		Currency:       Idr,
		Type:           Default,
		CheckoutAmount: float64(checkoutAmount),
	}

	var ws sync.WaitGroup

	loop := DEFAULT_USER_BALANCE / checkoutAmount
	totalFailed := 0

	for i := 0; i < loop; i++ {
		go func() {
			ws.Add(1)
			for {
				if err := DisburseUser(req, ctx, db); err != nil {
					totalFailed += 1
				} else {
					break
				}
			}
			ws.Done()
		}()
	}

	ws.Wait()

	paymentMethod, err := getPaymentMethod(DEFAULT_USERNAME, Idr, Default, db)
	fmt.Printf("Failure rate %.2f%% \n", float64(totalFailed)/float64(loop)*100)

	if err != nil {
		t.Fatal(err)
	}

	if paymentMethod.Balance != 0 {
		t.Fatalf("incorrect result %f", paymentMethod.Balance)
	}
}

func TestFloatingPoint(t *testing.T) {
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
		t.Fatal(err)
	}

	err = initData(db)

	if err != nil {
		t.Fatal(err)
	}

	checkoutAmount := 0.3
	mockPaymentMethod := PaymentMethod{
		UserId:   DEFAULT_USERNAME,
		Balance:  1,
		Type:     Default,
		Currency: Us,
	}
}
