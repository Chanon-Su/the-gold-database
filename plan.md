ย้อนกลับ: [README.md](README.md)

# Wolkflow
## Source data
- yfinance
## Bronze layer
    ที่ Broze layer เน้นการจัดเก็บแบบ raw ที่สุด เพื่อจะย้อนมาตรวจสอบถึงข้อมูลดิบได้ จึงไม่ได้ปรับอะไร นอกจาก เพิ่ม Batch_id และ เวลานำเข้า เพื่อคงความดังเดิมเอาไว้
- Request data from yfinance api
- Ingest the data
- Transform the data (+ Batct_id and ingest infomation)
- Load the data into bronze table
- Validation process
    - Pre validate
    - incremental load
    - Post validate

## Silver layer
    ที่ Silver layer เน้นที่การทำความสะอาดข้อมูล และ เตรียมสอบข้อมูลอย่างละเอียด เพื่อเตรียมความพร้อมการใช้งานให้ข้อมูล (make the data is ready to use)

- Extract the data from bronze table
- Transform process
    - ปรับ datatime เป็น timezone ประเทศไทย 
    - ...
- Validate process
    - Schema check -> จำนวน column, ประเภท datatype
    - Not Null check-> Open, High, Low, Close ห้ามเป็น null นอกจากนั้น ดูตามกรณี
    - Not Duplicate check-> Date + symbol ห้ามซ้ำกัน
    - Consistency check + Logic check->  ราคา High ต้องสูงกว่า low
    - Freshness check -> วันล่าสุดใน bronze layer ห่างจาก วันปัจจุบัน กี่วัน?
    - Volume check -> มีวันไหนที่ Volume = 0 หรือมีความผิดปกติ
    - Silent fail check -> ตรวจสอบข้อมูล เชิงรุก เพื่อหาความผิดปกติที่ซ่อนอยู่
- Load the data into silver table
- Create the validate reports

## Gold layer
    ที่ Gold layer เน้นที่เรียบเรียง เพิ่มเติม หรือกระบวนที่ทำให้ ข้อมูลจาก Silver พร้อมสำหรับการใช้งานกับ ตามที่ business ที่ต้องการ
- 