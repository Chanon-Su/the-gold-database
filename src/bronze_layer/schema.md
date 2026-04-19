Table bronze_yfinance_gold {
id,BIGSERIAL,PRIMARY KEY,ไอดีรันอัตโนมัติประจำแถว (Unique ID) (ไม่ ingest postgreSQL ทำให้เอง๗
symbol,TEXT,NOT NULL,สัญลักษณ์ตั๋ว เช่น 'GC=F'
datetime,TIMESTAMPTZ,NOT NULL,วันเวลาของแท่งเทียน (Market Timezone)
open,DOUBLE PRECISION,-,ราคาเปิด (Open Price)
high,DOUBLE PRECISION,-,ราคาสูงสุด (High Price)
low,DOUBLE PRECISION,-,ราคาต่ำสุด (Low Price)
close,DOUBLE PRECISION,-,ราคาปิด (Close Price)
volume,BIGINT,-,ปริมาณการซื้อขาย (Volume)
dividends,DOUBLE PRECISION,-,เงินปันผล (Dividends)
stock_splits,DOUBLE PRECISION,-,การแตกหุ้น (Stock Splits)
source,TEXT,DEFAULT 'yfinance',แหล่งที่มาของข้อมูล
ingested_at,TIMESTAMPTZ,DEFAULT NOW(),วันเวลาที่ข้อมูลถูกนำเข้าระบบ (System Time)
batch_id,TEXT,-,ID ระบุรอบการดึงข้อมูล (For Idempotency & Audit)
}

Bronze = Raw Data Zone
ต้อง:
เก็บ “ข้อมูลดิบที่สุด” + useable
ห้ามแก้ไข เพื่อให้คงไว้ความดังเดิมไว้ debug ย้อนหลังได้, เพิ่มได้อย่างเดียว และ datatype ที่เหมาะสมก่อน save to database
รองรับ schema change เผื่อไว้
ขั้นตอนคือ
1. Extract
2. Load to database (PostgreSQL)
3. Validate
ETL -> ELV