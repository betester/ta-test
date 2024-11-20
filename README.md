## Paper.id Take Home Test 

The following program handles when a user wants to disburse balance from virtual wallet. To run the program, simply run the following command

```go
go run main.go
```

Make sure to run `docker compose up -d` before running the server since the server needs to connect to a DB. The server runs on port 8080 with only `/api/disburse` endpoint open. The body of the requests are

```json
{
    "user_id": "mock_user",
    "currency": "Idr",
    "type": "Default",
    "checkout_amount": "0.110000"
}
```

Since the `user_id`, `currency`, and `type` are all hardcoded from the program.
