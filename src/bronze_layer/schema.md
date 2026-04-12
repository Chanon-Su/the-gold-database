Table bronze_yfinance_gold {
  id BIGSERIAL PRIMARY KEY ไอดี

  symbol TEXT สัญลักษณ์ UAXUSD
  datetime TIMESTAMPTZ วันเวลาของแท่งเทียน
  open DOUBLE PRECISION ราคาเปิดของแท่งเทียน
  high DOUBLE PRECISION ราคาสูงสุดของแท่งเทียน
  low DOUBLE PRECISION ราคาต่ำสุดของแท่งเทียน
  close DOUBLE PRECISION ราคาปิดของแท่งเทียน
  volume BIGINT ปริมาณ
  dividends DOUBLE PRECISION
  stock_splits DOUBLE PRECISION

  source TEXT DEFAULT 'yfinance' ดึงข้อมูลมาจากไหน 'yfinacnce'
  ingested_at TIMESTAMPTZ DEFAULT NOW() วันเวลาที่ดึงข้อมูลชุดนี้
  batch_id TEXT เลขระบุ ครั้งที่ดึงข้อมูล
}

Bronze = Raw Data Zone
ต้อง:
เก็บ “ข้อมูลดิบที่สุด”
ห้ามแก้ไข เพื่อให้คงไว้ความดังเดิมไว้ debug ย้อนหลังได้, เพิ่มได้อย่างเดียว
รองรับ schema change เผื่อไว้
ขั้นตอนคือ
1. Extract
2. Load to database (PostgreSQL)
3. Validate
ETL -> ELV