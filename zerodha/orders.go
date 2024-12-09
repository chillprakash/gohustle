package zerodha

// OrderType represents the type of order (MARKET, LIMIT, etc.)
type OrderType string

// OrderSide represents whether the order is BUY or SELL
type OrderSide string

// ProductType represents the product type (NRML, MIS, CNC)
type ProductType string

const (
	// Order Types
	OrderTypeMarket OrderType = "MARKET"
	OrderTypeLimit  OrderType = "LIMIT"
	OrderTypeSL     OrderType = "SL"
	OrderTypeSLM    OrderType = "SL-M"

	// Order Sides
	OrderSideBuy  OrderSide = "BUY"
	OrderSideSell OrderSide = "SELL"

	// Product Types
	ProductTypeNRML ProductType = "NRML"
	ProductTypeMIS  ProductType = "MIS"
	ProductTypeCNC  ProductType = "CNC"
)

// PlaceOrderRequest represents the parameters for placing an order
type PlaceOrderRequest struct {
	TradingSymbol string      `json:"trading_symbol"`
	Exchange      string      `json:"exchange"`
	OrderType     OrderType   `json:"order_type"`
	Side          OrderSide   `json:"side"`
	Quantity      int         `json:"quantity"`
	Price         float64     `json:"price,omitempty"`
	TriggerPrice  float64     `json:"trigger_price,omitempty"`
	Product       ProductType `json:"product"`
	Validity      string      `json:"validity,omitempty"`
	DisclosedQty  int         `json:"disclosed_qty,omitempty"`
	Tag           string      `json:"tag,omitempty"`
}
