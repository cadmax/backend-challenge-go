package domain_test

import (
	"encoding/json"
	"errors"
	"math"
	"math/big"
	"testing"

	"github.com/cadmax/backend-challenge-go/internal/domain"
)

func money(t testing.TB, amount string) domain.Money {
	t.Helper()
	m, err := domain.NewMoney(amount, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func minor(t testing.TB, value int64, currency string) domain.Money {
	t.Helper()
	m, err := domain.MoneyFromMinor(value, currency)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestMoneyParsingNormalizesExactMinorUnits(t *testing.T) {
	for _, tc := range []struct {
		input, normalized string
		units             int64
	}{
		{"0", "0.00", 0}, {"0.0", "0.00", 0}, {"000.00", "0.00", 0},
		{"25", "25.00", 2500}, {"01.2", "1.20", 120}, {"1.01", "1.01", 101},
		{"92233720368547758.07", "92233720368547758.07", math.MaxInt64},
	} {
		t.Run(tc.input, func(t *testing.T) {
			m := money(t, tc.input)
			if m.MinorUnits() != tc.units || m.Amount() != tc.normalized || m.Currency() != "BRL" {
				t.Fatalf("got %d %s %s", m.MinorUnits(), m.Amount(), m.Currency())
			}
		})
	}
}

func TestMoneyRejectsInvalidExternalRepresentations(t *testing.T) {
	for _, input := range []string{
		"", " ", " 1.00", "1.00 ", "NaN", "Infinity", "-Infinity", "1e2", "1E2",
		"-0", "-0.00", "-1", "+1", "1.001", "0.000", ".01", "1.", "1..0", "1,00", "１２.００",
	} {
		t.Run(input, func(t *testing.T) {
			if _, err := domain.NewMoney(input, "BRL"); !errors.Is(err, domain.ErrInvalidMoney) {
				t.Fatalf("expected invalid money for %q, got %v", input, err)
			}
		})
	}
	for _, input := range []string{"92233720368547758.08", "92233720368547759", "999999999999999999999999999999999999999"} {
		if _, err := domain.NewMoney(input, "BRL"); !errors.Is(err, domain.ErrOverflow) {
			t.Fatalf("expected overflow for %q, got %v", input, err)
		}
	}
	for _, currency := range []string{"", "brl", " BRL", "XYZ", "JPY", "BR", "BRLL"} {
		if _, err := domain.NewMoney("1", currency); !errors.Is(err, domain.ErrInvalidMoney) {
			t.Fatalf("expected invalid currency %q, got %v", currency, err)
		}
	}
	for _, currency := range []string{"BRL", "USD", "EUR"} {
		zero, err := domain.Zero(currency)
		if err != nil || zero.MinorUnits() != 0 || zero.Currency() != currency {
			t.Fatalf("currency-specific zero: %v %v", zero, err)
		}
	}
}

func TestMoneyArithmeticMatchesExactIntegerBoundaries(t *testing.T) {
	values := []int64{math.MinInt64, math.MinInt64 + 1, -100, -1, 0, 1, 100, math.MaxInt64 - 1, math.MaxInt64}
	for _, a := range values {
		for _, b := range values {
			left, right := minor(t, a, "BRL"), minor(t, b, "BRL")
			for _, tc := range []struct {
				name string
				op   func(domain.Money) (domain.Money, error)
				want *big.Int
			}{
				{"add", left.Add, new(big.Int).Add(big.NewInt(a), big.NewInt(b))},
				{"subtract", left.Sub, new(big.Int).Sub(big.NewInt(a), big.NewInt(b))},
			} {
				got, err := tc.op(right)
				if !tc.want.IsInt64() {
					if !errors.Is(err, domain.ErrOverflow) {
						t.Fatalf("%d %s %d must overflow: %v", a, tc.name, b, err)
					}
					continue
				}
				if err != nil || got.MinorUnits() != tc.want.Int64() {
					t.Fatalf("%d %s %d = %d, %v; want %s", a, tc.name, b, got.MinorUnits(), err, tc.want)
				}
			}
			comparison, err := left.Compare(right)
			if err != nil || comparison != big.NewInt(a).Cmp(big.NewInt(b)) {
				t.Fatalf("compare %d %d: %d, %v", a, b, comparison, err)
			}
		}
	}
	for _, value := range values {
		got, err := minor(t, value, "BRL").Negate()
		if value == math.MinInt64 {
			if !errors.Is(err, domain.ErrOverflow) {
				t.Fatalf("negating MinInt64: %v", err)
			}
		} else if err != nil || got.MinorUnits() != -value {
			t.Fatalf("negating %d: %d, %v", value, got.MinorUnits(), err)
		}
	}
}

func TestMoneyRequiresInitializedCompatibleCurrencies(t *testing.T) {
	brl, usd := money(t, "1"), minor(t, 100, "USD")
	for _, operation := range []func(domain.Money) (domain.Money, error){brl.Add, brl.Sub} {
		if _, err := operation(usd); !errors.Is(err, domain.ErrCurrencyMismatch) {
			t.Fatalf("currency mismatch: %v", err)
		}
		if _, err := operation(domain.Money{}); !errors.Is(err, domain.ErrInvalidMoney) {
			t.Fatalf("zero value: %v", err)
		}
	}
	if _, err := brl.Compare(usd); !errors.Is(err, domain.ErrCurrencyMismatch) {
		t.Fatal(err)
	}
	if _, err := (domain.Money{}).Negate(); !errors.Is(err, domain.ErrInvalidMoney) {
		t.Fatal(err)
	}
	if _, err := (domain.Money{}).Add(brl); !errors.Is(err, domain.ErrInvalidMoney) {
		t.Fatal(err)
	}
	if _, err := (domain.Money{}).Compare(brl); !errors.Is(err, domain.ErrInvalidMoney) {
		t.Fatal(err)
	}
}

func TestMoneyStrictJSONAndSignedOutput(t *testing.T) {
	for _, input := range []string{
		`null`, `1`, `"1"`, `[]`, `{}`, `{"amount":"1"}`, `{"currency":"BRL"}`,
		`{"amount":1,"currency":"BRL"}`, `{"amount":null,"currency":"BRL"}`,
		`{"amount":"1","currency":null}`, `{"amount":"1","currency":"BRL","extra":true}`,
		`{"amount":"1","amount":"2","currency":"BRL"}`,
		`{"amount":"1","currency":"BRL","currency":"USD"}`,
		`{"amount":"-1","currency":"BRL"}`, `{"amount":"1","currency":"BRL"} {}`,
		`{"amount":`,
	} {
		t.Run(input, func(t *testing.T) {
			m := money(t, "7")
			if err := m.UnmarshalJSON([]byte(input)); err == nil {
				t.Fatalf("accepted %s", input)
			}
			if m.MinorUnits() != 700 {
				t.Fatal("failed parsing mutated receiver")
			}
		})
	}
	var parsed domain.Money
	if err := json.Unmarshal([]byte(`{"currency":"BRL","amount":"001.2"}`), &parsed); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(parsed)
	if err != nil || string(encoded) != `{"amount":"1.20","currency":"BRL"}` {
		t.Fatalf("normalized JSON: %s, %v", encoded, err)
	}
	encoded, err = json.Marshal(minor(t, math.MinInt64, "BRL"))
	if err != nil || string(encoded) != `{"amount":"-92233720368547758.08","currency":"BRL"}` {
		t.Fatalf("signed minimum JSON: %s, %v", encoded, err)
	}
	if _, err := json.Marshal(domain.Money{}); !errors.Is(err, domain.ErrInvalidMoney) {
		t.Fatalf("uninitialized money JSON: %v", err)
	}
	var nilMoney *domain.Money
	if err := nilMoney.UnmarshalJSON([]byte(`{}`)); !errors.Is(err, domain.ErrInvalidMoney) {
		t.Fatal(err)
	}
}

func FuzzMoneyExternalRoundTrip(f *testing.F) {
	for _, input := range []string{"0", "01.2", "100.00", "92233720368547758.07", "NaN", "-1", "1e2"} {
		f.Add(input)
	}
	f.Fuzz(func(t *testing.T, input string) {
		m, err := domain.NewMoney(input, "BRL")
		if err != nil {
			return
		}
		encoded, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		var decoded domain.Money
		if err := json.Unmarshal(encoded, &decoded); err != nil || decoded != m {
			t.Fatalf("round trip %q: %v", input, err)
		}
	})
}
