package core

// Index represents a market index with its details
type Index struct {
	NameInOptions   string
	InstrumentToken string
	NameInIndices   string
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
			NameInOptions:   "NIFTY",
			InstrumentToken: "256265",
			NameInIndices:   "NIFTY 50",
		},
		SENSEX: Index{
			NameInOptions:   "SENSEX",
			InstrumentToken: "265",
			NameInIndices:   "SENSEX",
		},
		BANKNIFTY: Index{
			NameInOptions:   "BANKNIFTY",
			InstrumentToken: "260105",
			NameInIndices:   "NIFTY BANK",
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

// GetIndicesToSubscribeForIntraday returns all indices for intraday subscription
func (i Indices) GetIndicesToSubscribeForIntraday() []Index {
	return []Index{i.NIFTY, i.SENSEX, i.BANKNIFTY}
}

// GetIndicesToCollectData returns indices for data collection (excluding BANKNIFTY)
func (i Indices) GetIndicesToCollectData() []Index {
	return []Index{i.NIFTY, i.SENSEX}
}
