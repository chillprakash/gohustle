// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.35.2
// 	protoc        v5.29.0
// source: proto/tick.proto

package proto

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type TickData struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Basic info
	InstrumentToken uint32 `protobuf:"varint,1,opt,name=instrument_token,json=instrumentToken,proto3" json:"instrument_token,omitempty"`
	IsTradable      bool   `protobuf:"varint,2,opt,name=is_tradable,json=isTradable,proto3" json:"is_tradable,omitempty"`
	IsIndex         bool   `protobuf:"varint,3,opt,name=is_index,json=isIndex,proto3" json:"is_index,omitempty"`
	Mode            string `protobuf:"bytes,4,opt,name=mode,proto3" json:"mode,omitempty"`
	// Timestamps
	Timestamp     int64 `protobuf:"varint,5,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
	LastTradeTime int64 `protobuf:"varint,6,opt,name=last_trade_time,json=lastTradeTime,proto3" json:"last_trade_time,omitempty"`
	// Price and quantity
	LastPrice          float64 `protobuf:"fixed64,7,opt,name=last_price,json=lastPrice,proto3" json:"last_price,omitempty"`
	LastTradedQuantity uint32  `protobuf:"varint,8,opt,name=last_traded_quantity,json=lastTradedQuantity,proto3" json:"last_traded_quantity,omitempty"`
	TotalBuyQuantity   uint32  `protobuf:"varint,9,opt,name=total_buy_quantity,json=totalBuyQuantity,proto3" json:"total_buy_quantity,omitempty"`
	TotalSellQuantity  uint32  `protobuf:"varint,10,opt,name=total_sell_quantity,json=totalSellQuantity,proto3" json:"total_sell_quantity,omitempty"`
	VolumeTraded       uint32  `protobuf:"varint,11,opt,name=volume_traded,json=volumeTraded,proto3" json:"volume_traded,omitempty"`
	TotalBuy           uint32  `protobuf:"varint,12,opt,name=total_buy,json=totalBuy,proto3" json:"total_buy,omitempty"`
	TotalSell          uint32  `protobuf:"varint,13,opt,name=total_sell,json=totalSell,proto3" json:"total_sell,omitempty"`
	AverageTradePrice  float64 `protobuf:"fixed64,14,opt,name=average_trade_price,json=averageTradePrice,proto3" json:"average_trade_price,omitempty"`
	// OI related
	Oi        uint32                `protobuf:"varint,15,opt,name=oi,proto3" json:"oi,omitempty"`
	OiDayHigh uint32                `protobuf:"varint,16,opt,name=oi_day_high,json=oiDayHigh,proto3" json:"oi_day_high,omitempty"`
	OiDayLow  uint32                `protobuf:"varint,17,opt,name=oi_day_low,json=oiDayLow,proto3" json:"oi_day_low,omitempty"`
	NetChange float64               `protobuf:"fixed64,18,opt,name=net_change,json=netChange,proto3" json:"net_change,omitempty"`
	Ohlc      *TickData_OHLC        `protobuf:"bytes,19,opt,name=ohlc,proto3" json:"ohlc,omitempty"`
	Depth     *TickData_MarketDepth `protobuf:"bytes,20,opt,name=depth,proto3" json:"depth,omitempty"`
	// Additional metadata
	ChangePercent       float64 `protobuf:"fixed64,21,opt,name=change_percent,json=changePercent,proto3" json:"change_percent,omitempty"`
	LastTradePrice      float64 `protobuf:"fixed64,22,opt,name=last_trade_price,json=lastTradePrice,proto3" json:"last_trade_price,omitempty"`
	OpenInterest        uint32  `protobuf:"varint,23,opt,name=open_interest,json=openInterest,proto3" json:"open_interest,omitempty"`
	OpenInterestDayHigh uint32  `protobuf:"varint,24,opt,name=open_interest_day_high,json=openInterestDayHigh,proto3" json:"open_interest_day_high,omitempty"`
	OpenInterestDayLow  uint32  `protobuf:"varint,25,opt,name=open_interest_day_low,json=openInterestDayLow,proto3" json:"open_interest_day_low,omitempty"`
	// File management
	TargetFile         string `protobuf:"bytes,26,opt,name=target_file,json=targetFile,proto3" json:"target_file,omitempty"` // The parquet file this tick should be written to
	TickRecievedTime   int64  `protobuf:"varint,27,opt,name=tick_recieved_time,json=tickRecievedTime,proto3" json:"tick_recieved_time,omitempty"`
	TickStoredInDbTime int64  `protobuf:"varint,28,opt,name=tick_stored_in_db_time,json=tickStoredInDbTime,proto3" json:"tick_stored_in_db_time,omitempty"`
	IndexName          string `protobuf:"bytes,29,opt,name=index_name,json=indexName,proto3" json:"index_name,omitempty"`
}

func (x *TickData) Reset() {
	*x = TickData{}
	mi := &file_proto_tick_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *TickData) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TickData) ProtoMessage() {}

func (x *TickData) ProtoReflect() protoreflect.Message {
	mi := &file_proto_tick_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TickData.ProtoReflect.Descriptor instead.
func (*TickData) Descriptor() ([]byte, []int) {
	return file_proto_tick_proto_rawDescGZIP(), []int{0}
}

func (x *TickData) GetInstrumentToken() uint32 {
	if x != nil {
		return x.InstrumentToken
	}
	return 0
}

func (x *TickData) GetIsTradable() bool {
	if x != nil {
		return x.IsTradable
	}
	return false
}

func (x *TickData) GetIsIndex() bool {
	if x != nil {
		return x.IsIndex
	}
	return false
}

func (x *TickData) GetMode() string {
	if x != nil {
		return x.Mode
	}
	return ""
}

func (x *TickData) GetTimestamp() int64 {
	if x != nil {
		return x.Timestamp
	}
	return 0
}

func (x *TickData) GetLastTradeTime() int64 {
	if x != nil {
		return x.LastTradeTime
	}
	return 0
}

func (x *TickData) GetLastPrice() float64 {
	if x != nil {
		return x.LastPrice
	}
	return 0
}

func (x *TickData) GetLastTradedQuantity() uint32 {
	if x != nil {
		return x.LastTradedQuantity
	}
	return 0
}

func (x *TickData) GetTotalBuyQuantity() uint32 {
	if x != nil {
		return x.TotalBuyQuantity
	}
	return 0
}

func (x *TickData) GetTotalSellQuantity() uint32 {
	if x != nil {
		return x.TotalSellQuantity
	}
	return 0
}

func (x *TickData) GetVolumeTraded() uint32 {
	if x != nil {
		return x.VolumeTraded
	}
	return 0
}

func (x *TickData) GetTotalBuy() uint32 {
	if x != nil {
		return x.TotalBuy
	}
	return 0
}

func (x *TickData) GetTotalSell() uint32 {
	if x != nil {
		return x.TotalSell
	}
	return 0
}

func (x *TickData) GetAverageTradePrice() float64 {
	if x != nil {
		return x.AverageTradePrice
	}
	return 0
}

func (x *TickData) GetOi() uint32 {
	if x != nil {
		return x.Oi
	}
	return 0
}

func (x *TickData) GetOiDayHigh() uint32 {
	if x != nil {
		return x.OiDayHigh
	}
	return 0
}

func (x *TickData) GetOiDayLow() uint32 {
	if x != nil {
		return x.OiDayLow
	}
	return 0
}

func (x *TickData) GetNetChange() float64 {
	if x != nil {
		return x.NetChange
	}
	return 0
}

func (x *TickData) GetOhlc() *TickData_OHLC {
	if x != nil {
		return x.Ohlc
	}
	return nil
}

func (x *TickData) GetDepth() *TickData_MarketDepth {
	if x != nil {
		return x.Depth
	}
	return nil
}

func (x *TickData) GetChangePercent() float64 {
	if x != nil {
		return x.ChangePercent
	}
	return 0
}

func (x *TickData) GetLastTradePrice() float64 {
	if x != nil {
		return x.LastTradePrice
	}
	return 0
}

func (x *TickData) GetOpenInterest() uint32 {
	if x != nil {
		return x.OpenInterest
	}
	return 0
}

func (x *TickData) GetOpenInterestDayHigh() uint32 {
	if x != nil {
		return x.OpenInterestDayHigh
	}
	return 0
}

func (x *TickData) GetOpenInterestDayLow() uint32 {
	if x != nil {
		return x.OpenInterestDayLow
	}
	return 0
}

func (x *TickData) GetTargetFile() string {
	if x != nil {
		return x.TargetFile
	}
	return ""
}

func (x *TickData) GetTickRecievedTime() int64 {
	if x != nil {
		return x.TickRecievedTime
	}
	return 0
}

func (x *TickData) GetTickStoredInDbTime() int64 {
	if x != nil {
		return x.TickStoredInDbTime
	}
	return 0
}

func (x *TickData) GetIndexName() string {
	if x != nil {
		return x.IndexName
	}
	return ""
}

type BatchMetadata struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Timestamp  int64 `protobuf:"varint,1,opt,name=timestamp,proto3" json:"timestamp,omitempty"`                     // When this batch was created
	BatchSize  int32 `protobuf:"varint,2,opt,name=batch_size,json=batchSize,proto3" json:"batch_size,omitempty"`    // Number of ticks in batch
	RetryCount int32 `protobuf:"varint,3,opt,name=retry_count,json=retryCount,proto3" json:"retry_count,omitempty"` // Number of retry attempts for this batch
}

func (x *BatchMetadata) Reset() {
	*x = BatchMetadata{}
	mi := &file_proto_tick_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *BatchMetadata) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BatchMetadata) ProtoMessage() {}

func (x *BatchMetadata) ProtoReflect() protoreflect.Message {
	mi := &file_proto_tick_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BatchMetadata.ProtoReflect.Descriptor instead.
func (*BatchMetadata) Descriptor() ([]byte, []int) {
	return file_proto_tick_proto_rawDescGZIP(), []int{1}
}

func (x *BatchMetadata) GetTimestamp() int64 {
	if x != nil {
		return x.Timestamp
	}
	return 0
}

func (x *BatchMetadata) GetBatchSize() int32 {
	if x != nil {
		return x.BatchSize
	}
	return 0
}

func (x *BatchMetadata) GetRetryCount() int32 {
	if x != nil {
		return x.RetryCount
	}
	return 0
}

type TickBatch struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Ticks    []*TickData    `protobuf:"bytes,1,rep,name=ticks,proto3" json:"ticks,omitempty"`       // Array of tick data
	Metadata *BatchMetadata `protobuf:"bytes,2,opt,name=metadata,proto3" json:"metadata,omitempty"` // Metadata about this batch
}

func (x *TickBatch) Reset() {
	*x = TickBatch{}
	mi := &file_proto_tick_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *TickBatch) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TickBatch) ProtoMessage() {}

func (x *TickBatch) ProtoReflect() protoreflect.Message {
	mi := &file_proto_tick_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TickBatch.ProtoReflect.Descriptor instead.
func (*TickBatch) Descriptor() ([]byte, []int) {
	return file_proto_tick_proto_rawDescGZIP(), []int{2}
}

func (x *TickBatch) GetTicks() []*TickData {
	if x != nil {
		return x.Ticks
	}
	return nil
}

func (x *TickBatch) GetMetadata() *BatchMetadata {
	if x != nil {
		return x.Metadata
	}
	return nil
}

// OHLC data
type TickData_OHLC struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Open  float64 `protobuf:"fixed64,1,opt,name=open,proto3" json:"open,omitempty"`
	High  float64 `protobuf:"fixed64,2,opt,name=high,proto3" json:"high,omitempty"`
	Low   float64 `protobuf:"fixed64,3,opt,name=low,proto3" json:"low,omitempty"`
	Close float64 `protobuf:"fixed64,4,opt,name=close,proto3" json:"close,omitempty"`
}

func (x *TickData_OHLC) Reset() {
	*x = TickData_OHLC{}
	mi := &file_proto_tick_proto_msgTypes[3]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *TickData_OHLC) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TickData_OHLC) ProtoMessage() {}

func (x *TickData_OHLC) ProtoReflect() protoreflect.Message {
	mi := &file_proto_tick_proto_msgTypes[3]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TickData_OHLC.ProtoReflect.Descriptor instead.
func (*TickData_OHLC) Descriptor() ([]byte, []int) {
	return file_proto_tick_proto_rawDescGZIP(), []int{0, 0}
}

func (x *TickData_OHLC) GetOpen() float64 {
	if x != nil {
		return x.Open
	}
	return 0
}

func (x *TickData_OHLC) GetHigh() float64 {
	if x != nil {
		return x.High
	}
	return 0
}

func (x *TickData_OHLC) GetLow() float64 {
	if x != nil {
		return x.Low
	}
	return 0
}

func (x *TickData_OHLC) GetClose() float64 {
	if x != nil {
		return x.Close
	}
	return 0
}

// Market depth
type TickData_DepthItem struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Price    float64 `protobuf:"fixed64,1,opt,name=price,proto3" json:"price,omitempty"`
	Quantity uint32  `protobuf:"varint,2,opt,name=quantity,proto3" json:"quantity,omitempty"`
	Orders   uint32  `protobuf:"varint,3,opt,name=orders,proto3" json:"orders,omitempty"`
}

func (x *TickData_DepthItem) Reset() {
	*x = TickData_DepthItem{}
	mi := &file_proto_tick_proto_msgTypes[4]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *TickData_DepthItem) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TickData_DepthItem) ProtoMessage() {}

func (x *TickData_DepthItem) ProtoReflect() protoreflect.Message {
	mi := &file_proto_tick_proto_msgTypes[4]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TickData_DepthItem.ProtoReflect.Descriptor instead.
func (*TickData_DepthItem) Descriptor() ([]byte, []int) {
	return file_proto_tick_proto_rawDescGZIP(), []int{0, 1}
}

func (x *TickData_DepthItem) GetPrice() float64 {
	if x != nil {
		return x.Price
	}
	return 0
}

func (x *TickData_DepthItem) GetQuantity() uint32 {
	if x != nil {
		return x.Quantity
	}
	return 0
}

func (x *TickData_DepthItem) GetOrders() uint32 {
	if x != nil {
		return x.Orders
	}
	return 0
}

type TickData_MarketDepth struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Buy  []*TickData_DepthItem `protobuf:"bytes,1,rep,name=buy,proto3" json:"buy,omitempty"`
	Sell []*TickData_DepthItem `protobuf:"bytes,2,rep,name=sell,proto3" json:"sell,omitempty"`
}

func (x *TickData_MarketDepth) Reset() {
	*x = TickData_MarketDepth{}
	mi := &file_proto_tick_proto_msgTypes[5]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *TickData_MarketDepth) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TickData_MarketDepth) ProtoMessage() {}

func (x *TickData_MarketDepth) ProtoReflect() protoreflect.Message {
	mi := &file_proto_tick_proto_msgTypes[5]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TickData_MarketDepth.ProtoReflect.Descriptor instead.
func (*TickData_MarketDepth) Descriptor() ([]byte, []int) {
	return file_proto_tick_proto_rawDescGZIP(), []int{0, 2}
}

func (x *TickData_MarketDepth) GetBuy() []*TickData_DepthItem {
	if x != nil {
		return x.Buy
	}
	return nil
}

func (x *TickData_MarketDepth) GetSell() []*TickData_DepthItem {
	if x != nil {
		return x.Sell
	}
	return nil
}

var File_proto_tick_proto protoreflect.FileDescriptor

var file_proto_tick_proto_rawDesc = []byte{
	0x0a, 0x10, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x74, 0x69, 0x63, 0x6b, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x12, 0x02, 0x70, 0x62, 0x22, 0xe3, 0x0a, 0x0a, 0x08, 0x54, 0x69, 0x63, 0x6b, 0x44,
	0x61, 0x74, 0x61, 0x12, 0x29, 0x0a, 0x10, 0x69, 0x6e, 0x73, 0x74, 0x72, 0x75, 0x6d, 0x65, 0x6e,
	0x74, 0x5f, 0x74, 0x6f, 0x6b, 0x65, 0x6e, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x0f, 0x69,
	0x6e, 0x73, 0x74, 0x72, 0x75, 0x6d, 0x65, 0x6e, 0x74, 0x54, 0x6f, 0x6b, 0x65, 0x6e, 0x12, 0x1f,
	0x0a, 0x0b, 0x69, 0x73, 0x5f, 0x74, 0x72, 0x61, 0x64, 0x61, 0x62, 0x6c, 0x65, 0x18, 0x02, 0x20,
	0x01, 0x28, 0x08, 0x52, 0x0a, 0x69, 0x73, 0x54, 0x72, 0x61, 0x64, 0x61, 0x62, 0x6c, 0x65, 0x12,
	0x19, 0x0a, 0x08, 0x69, 0x73, 0x5f, 0x69, 0x6e, 0x64, 0x65, 0x78, 0x18, 0x03, 0x20, 0x01, 0x28,
	0x08, 0x52, 0x07, 0x69, 0x73, 0x49, 0x6e, 0x64, 0x65, 0x78, 0x12, 0x12, 0x0a, 0x04, 0x6d, 0x6f,
	0x64, 0x65, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x6d, 0x6f, 0x64, 0x65, 0x12, 0x1c,
	0x0a, 0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x18, 0x05, 0x20, 0x01, 0x28,
	0x03, 0x52, 0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x12, 0x26, 0x0a, 0x0f,
	0x6c, 0x61, 0x73, 0x74, 0x5f, 0x74, 0x72, 0x61, 0x64, 0x65, 0x5f, 0x74, 0x69, 0x6d, 0x65, 0x18,
	0x06, 0x20, 0x01, 0x28, 0x03, 0x52, 0x0d, 0x6c, 0x61, 0x73, 0x74, 0x54, 0x72, 0x61, 0x64, 0x65,
	0x54, 0x69, 0x6d, 0x65, 0x12, 0x1d, 0x0a, 0x0a, 0x6c, 0x61, 0x73, 0x74, 0x5f, 0x70, 0x72, 0x69,
	0x63, 0x65, 0x18, 0x07, 0x20, 0x01, 0x28, 0x01, 0x52, 0x09, 0x6c, 0x61, 0x73, 0x74, 0x50, 0x72,
	0x69, 0x63, 0x65, 0x12, 0x30, 0x0a, 0x14, 0x6c, 0x61, 0x73, 0x74, 0x5f, 0x74, 0x72, 0x61, 0x64,
	0x65, 0x64, 0x5f, 0x71, 0x75, 0x61, 0x6e, 0x74, 0x69, 0x74, 0x79, 0x18, 0x08, 0x20, 0x01, 0x28,
	0x0d, 0x52, 0x12, 0x6c, 0x61, 0x73, 0x74, 0x54, 0x72, 0x61, 0x64, 0x65, 0x64, 0x51, 0x75, 0x61,
	0x6e, 0x74, 0x69, 0x74, 0x79, 0x12, 0x2c, 0x0a, 0x12, 0x74, 0x6f, 0x74, 0x61, 0x6c, 0x5f, 0x62,
	0x75, 0x79, 0x5f, 0x71, 0x75, 0x61, 0x6e, 0x74, 0x69, 0x74, 0x79, 0x18, 0x09, 0x20, 0x01, 0x28,
	0x0d, 0x52, 0x10, 0x74, 0x6f, 0x74, 0x61, 0x6c, 0x42, 0x75, 0x79, 0x51, 0x75, 0x61, 0x6e, 0x74,
	0x69, 0x74, 0x79, 0x12, 0x2e, 0x0a, 0x13, 0x74, 0x6f, 0x74, 0x61, 0x6c, 0x5f, 0x73, 0x65, 0x6c,
	0x6c, 0x5f, 0x71, 0x75, 0x61, 0x6e, 0x74, 0x69, 0x74, 0x79, 0x18, 0x0a, 0x20, 0x01, 0x28, 0x0d,
	0x52, 0x11, 0x74, 0x6f, 0x74, 0x61, 0x6c, 0x53, 0x65, 0x6c, 0x6c, 0x51, 0x75, 0x61, 0x6e, 0x74,
	0x69, 0x74, 0x79, 0x12, 0x23, 0x0a, 0x0d, 0x76, 0x6f, 0x6c, 0x75, 0x6d, 0x65, 0x5f, 0x74, 0x72,
	0x61, 0x64, 0x65, 0x64, 0x18, 0x0b, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x0c, 0x76, 0x6f, 0x6c, 0x75,
	0x6d, 0x65, 0x54, 0x72, 0x61, 0x64, 0x65, 0x64, 0x12, 0x1b, 0x0a, 0x09, 0x74, 0x6f, 0x74, 0x61,
	0x6c, 0x5f, 0x62, 0x75, 0x79, 0x18, 0x0c, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x08, 0x74, 0x6f, 0x74,
	0x61, 0x6c, 0x42, 0x75, 0x79, 0x12, 0x1d, 0x0a, 0x0a, 0x74, 0x6f, 0x74, 0x61, 0x6c, 0x5f, 0x73,
	0x65, 0x6c, 0x6c, 0x18, 0x0d, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x09, 0x74, 0x6f, 0x74, 0x61, 0x6c,
	0x53, 0x65, 0x6c, 0x6c, 0x12, 0x2e, 0x0a, 0x13, 0x61, 0x76, 0x65, 0x72, 0x61, 0x67, 0x65, 0x5f,
	0x74, 0x72, 0x61, 0x64, 0x65, 0x5f, 0x70, 0x72, 0x69, 0x63, 0x65, 0x18, 0x0e, 0x20, 0x01, 0x28,
	0x01, 0x52, 0x11, 0x61, 0x76, 0x65, 0x72, 0x61, 0x67, 0x65, 0x54, 0x72, 0x61, 0x64, 0x65, 0x50,
	0x72, 0x69, 0x63, 0x65, 0x12, 0x0e, 0x0a, 0x02, 0x6f, 0x69, 0x18, 0x0f, 0x20, 0x01, 0x28, 0x0d,
	0x52, 0x02, 0x6f, 0x69, 0x12, 0x1e, 0x0a, 0x0b, 0x6f, 0x69, 0x5f, 0x64, 0x61, 0x79, 0x5f, 0x68,
	0x69, 0x67, 0x68, 0x18, 0x10, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x09, 0x6f, 0x69, 0x44, 0x61, 0x79,
	0x48, 0x69, 0x67, 0x68, 0x12, 0x1c, 0x0a, 0x0a, 0x6f, 0x69, 0x5f, 0x64, 0x61, 0x79, 0x5f, 0x6c,
	0x6f, 0x77, 0x18, 0x11, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x08, 0x6f, 0x69, 0x44, 0x61, 0x79, 0x4c,
	0x6f, 0x77, 0x12, 0x1d, 0x0a, 0x0a, 0x6e, 0x65, 0x74, 0x5f, 0x63, 0x68, 0x61, 0x6e, 0x67, 0x65,
	0x18, 0x12, 0x20, 0x01, 0x28, 0x01, 0x52, 0x09, 0x6e, 0x65, 0x74, 0x43, 0x68, 0x61, 0x6e, 0x67,
	0x65, 0x12, 0x25, 0x0a, 0x04, 0x6f, 0x68, 0x6c, 0x63, 0x18, 0x13, 0x20, 0x01, 0x28, 0x0b, 0x32,
	0x11, 0x2e, 0x70, 0x62, 0x2e, 0x54, 0x69, 0x63, 0x6b, 0x44, 0x61, 0x74, 0x61, 0x2e, 0x4f, 0x48,
	0x4c, 0x43, 0x52, 0x04, 0x6f, 0x68, 0x6c, 0x63, 0x12, 0x2e, 0x0a, 0x05, 0x64, 0x65, 0x70, 0x74,
	0x68, 0x18, 0x14, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x18, 0x2e, 0x70, 0x62, 0x2e, 0x54, 0x69, 0x63,
	0x6b, 0x44, 0x61, 0x74, 0x61, 0x2e, 0x4d, 0x61, 0x72, 0x6b, 0x65, 0x74, 0x44, 0x65, 0x70, 0x74,
	0x68, 0x52, 0x05, 0x64, 0x65, 0x70, 0x74, 0x68, 0x12, 0x25, 0x0a, 0x0e, 0x63, 0x68, 0x61, 0x6e,
	0x67, 0x65, 0x5f, 0x70, 0x65, 0x72, 0x63, 0x65, 0x6e, 0x74, 0x18, 0x15, 0x20, 0x01, 0x28, 0x01,
	0x52, 0x0d, 0x63, 0x68, 0x61, 0x6e, 0x67, 0x65, 0x50, 0x65, 0x72, 0x63, 0x65, 0x6e, 0x74, 0x12,
	0x28, 0x0a, 0x10, 0x6c, 0x61, 0x73, 0x74, 0x5f, 0x74, 0x72, 0x61, 0x64, 0x65, 0x5f, 0x70, 0x72,
	0x69, 0x63, 0x65, 0x18, 0x16, 0x20, 0x01, 0x28, 0x01, 0x52, 0x0e, 0x6c, 0x61, 0x73, 0x74, 0x54,
	0x72, 0x61, 0x64, 0x65, 0x50, 0x72, 0x69, 0x63, 0x65, 0x12, 0x23, 0x0a, 0x0d, 0x6f, 0x70, 0x65,
	0x6e, 0x5f, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x65, 0x73, 0x74, 0x18, 0x17, 0x20, 0x01, 0x28, 0x0d,
	0x52, 0x0c, 0x6f, 0x70, 0x65, 0x6e, 0x49, 0x6e, 0x74, 0x65, 0x72, 0x65, 0x73, 0x74, 0x12, 0x33,
	0x0a, 0x16, 0x6f, 0x70, 0x65, 0x6e, 0x5f, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x65, 0x73, 0x74, 0x5f,
	0x64, 0x61, 0x79, 0x5f, 0x68, 0x69, 0x67, 0x68, 0x18, 0x18, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x13,
	0x6f, 0x70, 0x65, 0x6e, 0x49, 0x6e, 0x74, 0x65, 0x72, 0x65, 0x73, 0x74, 0x44, 0x61, 0x79, 0x48,
	0x69, 0x67, 0x68, 0x12, 0x31, 0x0a, 0x15, 0x6f, 0x70, 0x65, 0x6e, 0x5f, 0x69, 0x6e, 0x74, 0x65,
	0x72, 0x65, 0x73, 0x74, 0x5f, 0x64, 0x61, 0x79, 0x5f, 0x6c, 0x6f, 0x77, 0x18, 0x19, 0x20, 0x01,
	0x28, 0x0d, 0x52, 0x12, 0x6f, 0x70, 0x65, 0x6e, 0x49, 0x6e, 0x74, 0x65, 0x72, 0x65, 0x73, 0x74,
	0x44, 0x61, 0x79, 0x4c, 0x6f, 0x77, 0x12, 0x1f, 0x0a, 0x0b, 0x74, 0x61, 0x72, 0x67, 0x65, 0x74,
	0x5f, 0x66, 0x69, 0x6c, 0x65, 0x18, 0x1a, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0a, 0x74, 0x61, 0x72,
	0x67, 0x65, 0x74, 0x46, 0x69, 0x6c, 0x65, 0x12, 0x2c, 0x0a, 0x12, 0x74, 0x69, 0x63, 0x6b, 0x5f,
	0x72, 0x65, 0x63, 0x69, 0x65, 0x76, 0x65, 0x64, 0x5f, 0x74, 0x69, 0x6d, 0x65, 0x18, 0x1b, 0x20,
	0x01, 0x28, 0x03, 0x52, 0x10, 0x74, 0x69, 0x63, 0x6b, 0x52, 0x65, 0x63, 0x69, 0x65, 0x76, 0x65,
	0x64, 0x54, 0x69, 0x6d, 0x65, 0x12, 0x32, 0x0a, 0x16, 0x74, 0x69, 0x63, 0x6b, 0x5f, 0x73, 0x74,
	0x6f, 0x72, 0x65, 0x64, 0x5f, 0x69, 0x6e, 0x5f, 0x64, 0x62, 0x5f, 0x74, 0x69, 0x6d, 0x65, 0x18,
	0x1c, 0x20, 0x01, 0x28, 0x03, 0x52, 0x12, 0x74, 0x69, 0x63, 0x6b, 0x53, 0x74, 0x6f, 0x72, 0x65,
	0x64, 0x49, 0x6e, 0x44, 0x62, 0x54, 0x69, 0x6d, 0x65, 0x12, 0x1d, 0x0a, 0x0a, 0x69, 0x6e, 0x64,
	0x65, 0x78, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x1d, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x69,
	0x6e, 0x64, 0x65, 0x78, 0x4e, 0x61, 0x6d, 0x65, 0x1a, 0x56, 0x0a, 0x04, 0x4f, 0x48, 0x4c, 0x43,
	0x12, 0x12, 0x0a, 0x04, 0x6f, 0x70, 0x65, 0x6e, 0x18, 0x01, 0x20, 0x01, 0x28, 0x01, 0x52, 0x04,
	0x6f, 0x70, 0x65, 0x6e, 0x12, 0x12, 0x0a, 0x04, 0x68, 0x69, 0x67, 0x68, 0x18, 0x02, 0x20, 0x01,
	0x28, 0x01, 0x52, 0x04, 0x68, 0x69, 0x67, 0x68, 0x12, 0x10, 0x0a, 0x03, 0x6c, 0x6f, 0x77, 0x18,
	0x03, 0x20, 0x01, 0x28, 0x01, 0x52, 0x03, 0x6c, 0x6f, 0x77, 0x12, 0x14, 0x0a, 0x05, 0x63, 0x6c,
	0x6f, 0x73, 0x65, 0x18, 0x04, 0x20, 0x01, 0x28, 0x01, 0x52, 0x05, 0x63, 0x6c, 0x6f, 0x73, 0x65,
	0x1a, 0x55, 0x0a, 0x09, 0x44, 0x65, 0x70, 0x74, 0x68, 0x49, 0x74, 0x65, 0x6d, 0x12, 0x14, 0x0a,
	0x05, 0x70, 0x72, 0x69, 0x63, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x01, 0x52, 0x05, 0x70, 0x72,
	0x69, 0x63, 0x65, 0x12, 0x1a, 0x0a, 0x08, 0x71, 0x75, 0x61, 0x6e, 0x74, 0x69, 0x74, 0x79, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x08, 0x71, 0x75, 0x61, 0x6e, 0x74, 0x69, 0x74, 0x79, 0x12,
	0x16, 0x0a, 0x06, 0x6f, 0x72, 0x64, 0x65, 0x72, 0x73, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0d, 0x52,
	0x06, 0x6f, 0x72, 0x64, 0x65, 0x72, 0x73, 0x1a, 0x63, 0x0a, 0x0b, 0x4d, 0x61, 0x72, 0x6b, 0x65,
	0x74, 0x44, 0x65, 0x70, 0x74, 0x68, 0x12, 0x28, 0x0a, 0x03, 0x62, 0x75, 0x79, 0x18, 0x01, 0x20,
	0x03, 0x28, 0x0b, 0x32, 0x16, 0x2e, 0x70, 0x62, 0x2e, 0x54, 0x69, 0x63, 0x6b, 0x44, 0x61, 0x74,
	0x61, 0x2e, 0x44, 0x65, 0x70, 0x74, 0x68, 0x49, 0x74, 0x65, 0x6d, 0x52, 0x03, 0x62, 0x75, 0x79,
	0x12, 0x2a, 0x0a, 0x04, 0x73, 0x65, 0x6c, 0x6c, 0x18, 0x02, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x16,
	0x2e, 0x70, 0x62, 0x2e, 0x54, 0x69, 0x63, 0x6b, 0x44, 0x61, 0x74, 0x61, 0x2e, 0x44, 0x65, 0x70,
	0x74, 0x68, 0x49, 0x74, 0x65, 0x6d, 0x52, 0x04, 0x73, 0x65, 0x6c, 0x6c, 0x22, 0x6d, 0x0a, 0x0d,
	0x42, 0x61, 0x74, 0x63, 0x68, 0x4d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x12, 0x1c, 0x0a,
	0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x18, 0x01, 0x20, 0x01, 0x28, 0x03,
	0x52, 0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x12, 0x1d, 0x0a, 0x0a, 0x62,
	0x61, 0x74, 0x63, 0x68, 0x5f, 0x73, 0x69, 0x7a, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x05, 0x52,
	0x09, 0x62, 0x61, 0x74, 0x63, 0x68, 0x53, 0x69, 0x7a, 0x65, 0x12, 0x1f, 0x0a, 0x0b, 0x72, 0x65,
	0x74, 0x72, 0x79, 0x5f, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x18, 0x03, 0x20, 0x01, 0x28, 0x05, 0x52,
	0x0a, 0x72, 0x65, 0x74, 0x72, 0x79, 0x43, 0x6f, 0x75, 0x6e, 0x74, 0x22, 0x5e, 0x0a, 0x09, 0x54,
	0x69, 0x63, 0x6b, 0x42, 0x61, 0x74, 0x63, 0x68, 0x12, 0x22, 0x0a, 0x05, 0x74, 0x69, 0x63, 0x6b,
	0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x0c, 0x2e, 0x70, 0x62, 0x2e, 0x54, 0x69, 0x63,
	0x6b, 0x44, 0x61, 0x74, 0x61, 0x52, 0x05, 0x74, 0x69, 0x63, 0x6b, 0x73, 0x12, 0x2d, 0x0a, 0x08,
	0x6d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x11,
	0x2e, 0x70, 0x62, 0x2e, 0x42, 0x61, 0x74, 0x63, 0x68, 0x4d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74,
	0x61, 0x52, 0x08, 0x6d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x42, 0x09, 0x5a, 0x07, 0x2e,
	0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_proto_tick_proto_rawDescOnce sync.Once
	file_proto_tick_proto_rawDescData = file_proto_tick_proto_rawDesc
)

func file_proto_tick_proto_rawDescGZIP() []byte {
	file_proto_tick_proto_rawDescOnce.Do(func() {
		file_proto_tick_proto_rawDescData = protoimpl.X.CompressGZIP(file_proto_tick_proto_rawDescData)
	})
	return file_proto_tick_proto_rawDescData
}

var file_proto_tick_proto_msgTypes = make([]protoimpl.MessageInfo, 6)
var file_proto_tick_proto_goTypes = []any{
	(*TickData)(nil),             // 0: pb.TickData
	(*BatchMetadata)(nil),        // 1: pb.BatchMetadata
	(*TickBatch)(nil),            // 2: pb.TickBatch
	(*TickData_OHLC)(nil),        // 3: pb.TickData.OHLC
	(*TickData_DepthItem)(nil),   // 4: pb.TickData.DepthItem
	(*TickData_MarketDepth)(nil), // 5: pb.TickData.MarketDepth
}
var file_proto_tick_proto_depIdxs = []int32{
	3, // 0: pb.TickData.ohlc:type_name -> pb.TickData.OHLC
	5, // 1: pb.TickData.depth:type_name -> pb.TickData.MarketDepth
	0, // 2: pb.TickBatch.ticks:type_name -> pb.TickData
	1, // 3: pb.TickBatch.metadata:type_name -> pb.BatchMetadata
	4, // 4: pb.TickData.MarketDepth.buy:type_name -> pb.TickData.DepthItem
	4, // 5: pb.TickData.MarketDepth.sell:type_name -> pb.TickData.DepthItem
	6, // [6:6] is the sub-list for method output_type
	6, // [6:6] is the sub-list for method input_type
	6, // [6:6] is the sub-list for extension type_name
	6, // [6:6] is the sub-list for extension extendee
	0, // [0:6] is the sub-list for field type_name
}

func init() { file_proto_tick_proto_init() }
func file_proto_tick_proto_init() {
	if File_proto_tick_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_proto_tick_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   6,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_proto_tick_proto_goTypes,
		DependencyIndexes: file_proto_tick_proto_depIdxs,
		MessageInfos:      file_proto_tick_proto_msgTypes,
	}.Build()
	File_proto_tick_proto = out.File
	file_proto_tick_proto_rawDesc = nil
	file_proto_tick_proto_goTypes = nil
	file_proto_tick_proto_depIdxs = nil
}
