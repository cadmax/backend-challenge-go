package domain

import (
	"bytes"
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"strconv"
	"strings"
)

var supportedCurrencies = []string{"BRL", "USD", "EUR"}

var (
	ErrInvalidMoney     = errors.New("invalid money")
	ErrCurrencyMismatch = errors.New("currency mismatch")
	ErrOverflow         = errors.New("monetary overflow")
)

// Money is immutable and stores signed, exact minor units. Its zero Go value is
// invalid; use Zero when a currency-specific zero is needed.
type Money struct {
	minor    int64
	currency string
}

// NewMoney parses a nonnegative external amount without floating-point arithmetic.
// Whole units, one or two decimal places and leading zeros are accepted and
// normalized by Amount. Signs, whitespace, exponents and excess scale are invalid.
func NewMoney(amount, currency string) (Money, error) {
	if !supportedCurrency(currency) || amount == "" {
		return Money{}, ErrInvalidMoney
	}
	parts := strings.Split(amount, ".")
	if len(parts) > 2 || parts[0] == "" {
		return Money{}, ErrInvalidMoney
	}
	for _, part := range parts {
		if part == "" {
			return Money{}, ErrInvalidMoney
		}
		for _, c := range part {
			if c < '0' || c > '9' {
				return Money{}, ErrInvalidMoney
			}
		}
	}
	fraction := "00"
	if len(parts) == 2 {
		if len(parts[1]) > 2 {
			return Money{}, ErrInvalidMoney
		}
		fraction = parts[1]
		if len(fraction) == 1 {
			fraction += "0"
		}
	}
	units, err := strconv.ParseUint(parts[0]+fraction, 10, 64)
	if err != nil || units > math.MaxInt64 {
		return Money{}, ErrOverflow
	}
	return Money{minor: int64(units), currency: currency}, nil
}

// MoneyFromMinor is the persistence/internal arithmetic constructor. Unlike
// NewMoney, it accepts negative values, including math.MinInt64.
func MoneyFromMinor(minor int64, currency string) (Money, error) {
	if !supportedCurrency(currency) {
		return Money{}, ErrInvalidMoney
	}
	return Money{minor: minor, currency: currency}, nil
}

func Zero(currency string) (Money, error) { return MoneyFromMinor(0, currency) }
func (m Money) MinorUnits() int64         { return m.minor }
func (m Money) Currency() string          { return m.currency }

func (m Money) Validate() error {
	if !supportedCurrency(m.currency) {
		return ErrInvalidMoney
	}
	return nil
}

func supportedCurrency(currency string) bool {
	return slices.Contains(supportedCurrencies, currency)
}

func (m Money) compatible(other Money) error {
	if err := m.Validate(); err != nil {
		return err
	}
	if err := other.Validate(); err != nil {
		return err
	}
	if m.currency != other.currency {
		return ErrCurrencyMismatch
	}
	return nil
}

func (m Money) Add(other Money) (Money, error) {
	if err := m.compatible(other); err != nil {
		return Money{}, err
	}
	if (other.minor > 0 && m.minor > math.MaxInt64-other.minor) ||
		(other.minor < 0 && m.minor < math.MinInt64-other.minor) {
		return Money{}, ErrOverflow
	}
	return Money{minor: m.minor + other.minor, currency: m.currency}, nil
}

func (m Money) Sub(other Money) (Money, error) {
	if err := m.compatible(other); err != nil {
		return Money{}, err
	}
	// Check subtraction directly: negating MinInt64 first would reject valid
	// differences such as MinInt64 - MinInt64.
	if (other.minor > 0 && m.minor < math.MinInt64+other.minor) ||
		(other.minor < 0 && m.minor > math.MaxInt64+other.minor) {
		return Money{}, ErrOverflow
	}
	return Money{minor: m.minor - other.minor, currency: m.currency}, nil
}

func (m Money) Negate() (Money, error) {
	if err := m.Validate(); err != nil {
		return Money{}, err
	}
	if m.minor == math.MinInt64 {
		return Money{}, ErrOverflow
	}
	return Money{minor: -m.minor, currency: m.currency}, nil
}

func (m Money) Compare(other Money) (int, error) {
	if err := m.compatible(other); err != nil {
		return 0, err
	}
	return cmp.Compare(m.minor, other.minor), nil
}

func (m Money) Amount() string {
	negative := m.minor < 0
	magnitude := uint64(m.minor)
	if negative {
		magnitude = uint64(-(m.minor + 1)) + 1
	}
	amount := fmt.Sprintf("%d.%02d", magnitude/100, magnitude%100)
	if negative {
		return "-" + amount
	}
	return amount
}

// MarshalJSON permits signed internal differences, e.g. reconciliation output.
// UnmarshalJSON deliberately uses the stricter external, nonnegative contract.
func (m Money) MarshalJSON() ([]byte, error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(struct {
		Amount   string `json:"amount"`
		Currency string `json:"currency"`
	}{m.Amount(), m.currency})
}

func (m *Money) UnmarshalJSON(data []byte) error {
	if m == nil {
		return ErrInvalidMoney
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return ErrInvalidMoney
	}
	fields := make(map[string]string, 2)
	for decoder.More() {
		token, err = decoder.Token()
		if err != nil {
			return ErrInvalidMoney
		}
		key, ok := token.(string)
		if !ok || (key != "amount" && key != "currency") {
			return ErrInvalidMoney
		}
		if _, duplicate := fields[key]; duplicate {
			return ErrInvalidMoney
		}
		var value *string
		if err := decoder.Decode(&value); err != nil || value == nil {
			return ErrInvalidMoney
		}
		fields[key] = *value
	}
	if _, err := decoder.Token(); err != nil {
		return ErrInvalidMoney
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return ErrInvalidMoney
	}
	parsed, err := NewMoney(fields["amount"], fields["currency"])
	if err != nil {
		return err
	}
	*m = parsed
	return nil
}
