from flask import Flask, render_template
from sqlalchemy import create_engine
import pandas as pd

app = Flask(__name__)

# MySQL 서버 정보 설정
host="localhost"
user="testuser"
password="1234"
database="TESTDB"

# SQLAlchemy 엔진 생성
engine_url = f'mysql+pymysql://{user}:{password}@{host}/{database}'
engine = create_engine(engine_url, echo=True)


@app.route('/')
def home():

    # 쿼리 실행 및 결과 DataFrame으로 변환
    query = "SELECT * FROM t5_table"
    df = pd.read_sql(query, engine)
    
    return render_template('home.html', 
                          tables=[df.to_html(classes='data')], 
                          titles=df.columns.values)


if __name__ == '__main__':
    app.run(debug=True)


