package zerodha

// Index represents a market index with its details
type Index struct {
	ExchangeName    string
	InstrumentToken string
}

// Indices represents all available market indices
type Indices struct {
	NIFTY     Index
	SENSEX    Index
	BANKNIFTY Index
}

// GetIndices returns a singleton instance of Indices
var GetIndices = func() Indices {
	return Indices{
		NIFTY: Index{
			ExchangeName:    "NIFTY 50",
			InstrumentToken: "256265",
		},
		SENSEX: Index{
			ExchangeName:    "SENSEX",
			InstrumentToken: "265",
		},
		BANKNIFTY: Index{
			ExchangeName:    "NIFTY BANK",
			InstrumentToken: "260105",
		},
	}
}

// GetAllIndices returns a slice of all indices
func (i Indices) GetAllIndices() []Index {
	return []Index{i.NIFTY, i.SENSEX, i.BANKNIFTY}
}

// GetAllInstrumentTokens returns a slice of all instrument tokens
func (i Indices) GetAllInstrumentTokens() []string {
	return []string{i.NIFTY.InstrumentToken, i.SENSEX.InstrumentToken, i.BANKNIFTY.InstrumentToken}
}

// GetAllNames returns a slice of all index names
func (i Indices) GetAllNames() []string {
	return []string{"NIFTY", "SENSEX", "BANKNIFTY"}
}

// GetIndexByName returns an Index by its name
func (i Indices) GetIndexByName(name string) *Index {
	switch name {
	case "NIFTY":
		return &i.NIFTY
	case "SENSEX":
		return &i.SENSEX
	case "BANKNIFTY":
		return &i.BANKNIFTY
	default:
		return nil
	}
}

// GetIndexByExchangeName returns an Index by its exchange name
func (i Indices) GetIndexByExchangeName(exchangeName string) *Index {
	switch exchangeName {
	case "NIFTY 50":
		return &i.NIFTY
	case "SENSEX":
		return &i.SENSEX
	case "NIFTY BANK":
		return &i.BANKNIFTY
	default:
		return nil
	}
}
