ย้อนกลับ: [README.md](README.md)

## ฟังชั่น pre_validate
    ก่อนการ load เข้า database ก็ควรจะมีการ validate เบื้องต้นก่อน ไม่ใช่ว่านำเข้าไปค่อยมาเช็ค จึงเป็น pre_validate ที่จะเป็น Quick checking อย่างเร็ว และ พยายามที่จะป้องกัน silent failed ที่ตรวจจับได้อยาก เช่น ถ้ามีAPI ต้นทางมีการเพิ่ม/ลด column, ข้อมูลเข้ามาแต่ว่าเป็น null เป็นต้น
### หัวข้อที่ตรวจสอบ
1. empty check, row < 0, col<0, len(col).
2. required_Columns check, the main column must have in ingestion.
3. Data type check. the df have same datatype like previous lot before transform.
4. duplicate check
5. Null check(loop check each column), some column can have null some column must no null.

## ฟังชั่น post_validate
    หลังการ load เข้า database ก็ควรจะมีการ validate อีกครั้งว่า ข้อมูลที่ load เข้าไป ครบ ถูกต้อง ไม่มีผิดพลาด จึงได้ทำให้ code เช็คด้วยเลข batch_id เพื่อจะได้ ตรวจสอบย้อนหลังได้ (traceability) และหลังจาก post_validate เสร็จก็ได้ เพิ่ม action ว่า ถ้าการทำ post_validate ไม่ผ่าน ให้เลือกเลยว่าจะลบ batch นั้นทันทีมั้ย?
### หัวข้อที่ตรวจสอบ
1. จำนวน rows ที่นำเข้า และ จำนวน rows ใน database โดยใช้ batch_id เป็นตรวจเชื่อม
2. นับจำนวน null ทั้งก่อนเข้า และ หลังเข้า เพื่อดูว่า ไม่มีการส่ง null row เข้าไปใน database
3. ตรวจสอบ value ข้างใน ว่า ที่นำเข้าไปตรงมั้ย? แต่มีปัญหา คือ การจูน datatype เช่น float บน python นั้นละเอียดไม่เท่า float บน postgreSQL จึงต้องหาวิธีตรวจโดยคร่าวๆ คือการนับแค่ ครึ่งแรกก่อน เช่น 1.73241468 และ 1.732 ก็เลยนับ แค่ 4 ตัวแรกพอคือ 1.732 == 1.732 และให้เป็น PASS with condition เอาไว้

## ฟังชั่น incremental_load
    ฟังชั่นที่ ทำลดความซ้ำซ้อนในการนำเข้าข้อมูล คือการ เช็คจาก PK ใน database ก่อน และ unselect จาก df ก่อน load เพื่อให้ได้ pk ที่ใหม่ หรือก็คือ ข้อมูลที่ยังไม่มี เท่านั้น อันที่เคยมีก็ถูก unselect ไปหมด. ใช้ ฟังชั่นนี้ หลัง pre_validate ก่อนการ load. นอกจากนี้ยังเป็นการช่วยเก็บตกข้อมูลที่ อาจจะสูญหายได้ให้กลับมาครบสมบูรณ์ เช่น ถ้าไม่ได้ load มา 4 วัน ใช้ incremental ก็จะได้ครบส่วนที่ขาดไป
### หัวข้อที่ตรวจสอบ
1. query primary key จาก database ก่อน
2. outer join เพื่อให้ได้ pk ที่ไม่เคยมีใน database มาก่อน (แต่ ใน code ไม่ได้ใช้วิธี outer join นะ)
3. return กลับไป main พร้อม load

## ฟังชั่น schema_check
    เป็นต้นแบบของทั้ง pre_validate และ post_validate เท่านั้นไม่มีอะไร