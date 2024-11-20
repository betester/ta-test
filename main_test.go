package main

import (
	"context"
	"fmt"

	"sync"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/shopspring/decimal"
)

//TODO: test in here the following
// 1. do concurrent update on the price with the expected price (done)
// 2. do floating point related issues  testing (done)
// 3. do testing on negative number (done)

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

	checkoutAmount := "250"

	ctx := context.Background()

	req := DisburseRequest{
		UserId:         DEFAULT_USERNAME,
		Currency:       Idr,
		Type:           Default,
		CheckoutAmount: checkoutAmount,
	}

	var ws sync.WaitGroup

	loop := 40
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

	if paymentMethod.Balance != "0.0000" {
		t.Fatalf("incorrect result %s", paymentMethod.Balance)
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

	checkoutAmount := "0.33"

	mockPaymentMethod := PaymentMethod{
		UserId:   DEFAULT_USERNAME,
		Balance:  "1.00",
		Type:     Default,
		Currency: Us,
	}

	addPaymentMethod(mockPaymentMethod, db)

	req := DisburseRequest{
		UserId:         DEFAULT_USERNAME,
		Currency:       Us,
		Type:           Default,
		CheckoutAmount: checkoutAmount,
	}

	ctx := context.Background()

	if err := DisburseUser(req, ctx, db); err != nil {
		t.Fatalf(err.Error())
	}
	if err := DisburseUser(req, ctx, db); err != nil {
		t.Fatalf(err.Error())
	}
	req.CheckoutAmount = "0.34"
	if err := DisburseUser(req, ctx, db); err != nil {
		t.Fatalf(err.Error())
	}

	paymentMethod, err := getPaymentMethod(DEFAULT_USERNAME, Us, Default, db)

	balance, err := decimal.NewFromString(paymentMethod.Balance)

	if err != nil {
		t.Fatalf(err.Error())
	}

	if !balance.Equal(decimal.NewFromInt(0)) {
		fmt.Println(paymentMethod)
		t.Fatalf("incorrect result %s", balance)
	}
}

func TestNegativeNumber(t *testing.T) {
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

	checkoutAmount := "-1.00"
	liraBalance := "1000"

	mockPaymentMethod := PaymentMethod{
		UserId:   DEFAULT_USERNAME,
		Balance:  liraBalance,
		Type:     Default,
		Currency: Lira,
	}

	addPaymentMethod(mockPaymentMethod, db)

	req := DisburseRequest{
		UserId:         DEFAULT_USERNAME,
		Currency:       Lira,
		Type:           Default,
		CheckoutAmount: checkoutAmount,
	}

	ctx := context.Background()

	if err := DisburseUser(req, ctx, db); err == nil {
		t.Fatalf("Should not allow neagitve number")
	}

	paymentMethod, err := getPaymentMethod(DEFAULT_USERNAME, Lira, Default, db)
	result, _ := decimal.NewFromString(paymentMethod.Balance)
	liraDecimal, _ := decimal.NewFromString(liraBalance)

	if !result.Equal(liraDecimal) {
		t.Fatalf("Should not allow neagitve number")
	}
}
