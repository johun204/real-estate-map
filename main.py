import os
import asyncio
import sys
import json
import requests
import aiohttp
import math
import xml.etree.ElementTree as ET
from datetime import date, timedelta, datetime, timezone

# 1. 설정
KAKAO_API_KEY = os.getenv('KAKAO_API_KEY')
DATAGOKR_API_KEY = os.getenv('DATAGOKR_API_KEY')
CACHE_FILE_PATH = 'data/address_cache.json' # 캐시 파일 경로

# 전역 캐시 저장소 (주소: (장소명, 위도, 경도))
ADDRESS_CACHE = {}

# -------------------- HTTP 요청 -------------------- #
async def fetch_post(session, url, data=None, json_body=None, headers=None):
    try:
        kwargs = {"timeout": 10}
        if headers:
            kwargs["headers"] = headers

        if json_body is not None:
            kwargs["json"] = json_body
        elif data is not None:
            kwargs["data"] = data

        async with session.post(url, **kwargs) as response:
            return await response.text()
    except Exception as e:
        return f"ERROR:{str(e)}"


async def fetch_page(session, url, params, semaphore):
    """단일 페이지의 데이터를 비동기로 요청하고 파싱하는 함수"""
    async with semaphore:  # 동시 요청 수 제한
        async with session.get(url, params=params) as response:
            if response.status != 200:
                print(f"[오류] {params['pageNo']}페이지 통신 실패 (상태 코드: {response.status})")
                return []
            
            xml_data = await response.text()
            root = ET.fromstring(xml_data)
            
            # API 자체 에러 메세지 처리
            result_code = root.find('.//resultCode')
            if result_code is not None and result_code.text != '000':
                result_msg = root.find('.//resultMsg').text
                print(f"[API 오류 - {params['pageNo']}페이지] {result_msg}")
                return []

            items = root.findall('.//item')
            page_data = []
            for item in items:
                item_dict = {}
                for child in item:
                    item_dict[child.tag] = child.text.strip() if child.text else ""
                page_data.append(item_dict)
                
            return page_data

async def fetch_all_apt_trade_data_async(api_key, lawd_cd, deal_ymd, num_of_rows=10):
    url = "https://apis.data.go.kr/1613000/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade"
    all_items = []
    
    # 동시 요청 개수 제한 (공공데이터 API는 보통 5~10 정도로 제한하는 것이 안전합니다)
    semaphore = asyncio.Semaphore(5) 
    
    async with aiohttp.ClientSession() as session:
        # 1. 1페이지를 먼저 요청하여 전체 데이터 수(totalCount) 파악
        params = {
            "serviceKey": api_key, 
            "LAWD_CD": lawd_cd,
            "DEAL_YMD": deal_ymd,
            "numOfRows": str(num_of_rows),
            "pageNo": "1"
        }
        
        async with session.get(url, params=params) as response:
            xml_data = await response.text()
            root = ET.fromstring(xml_data)
            
            total_count_element = root.find('.//totalCount')
            if total_count_element is None:
                print("[알림] 데이터를 찾을 수 없거나 API 응답이 올바르지 않습니다.")
                return []
                
            total_count = int(total_count_element.text)
            
            # 1페이지 데이터 파싱해서 결과 리스트에 추가
            first_page_items = root.findall('.//item')
            for item in first_page_items:
                item_dict = {}
                for child in item:
                    item_dict[child.tag] = child.text.strip() if child.text else ""
                all_items.append(item_dict)

        # 2. 전체 페이지 수 계산
        total_pages = math.ceil(total_count / num_of_rows)
        
        # 3. 2페이지부터 마지막 페이지까지 비동기 작업(Task) 생성
        tasks = []
        for page in range(2, total_pages + 1):
            page_params = params.copy()
            page_params["pageNo"] = str(page)
            # 코루틴 객체 생성
            task = fetch_page(session, url, page_params, semaphore)
            tasks.append(task)
            
        # 4. 생성된 모든 작업을 동시에 실행 (Gather)
        if tasks:
            results = await asyncio.gather(*tasks)
            
            # 각 페이지의 결과 리스트를 메인 리스트에 병합
            for page_result in results:
                all_items.extend(page_result)

    print(f"[완료] {lawd_cd}, {deal_ymd}, {len(all_items)}개의 데이터를 성공적으로 수집했습니다.")
    return all_items



# 2. 카카오 좌표 변환 함수 (캐시 적용)
async def get_lat_lon(session, address):
    # 1. 캐시에 있는지 먼저 확인 (메모리 조회)
    if address in ADDRESS_CACHE:
        return ADDRESS_CACHE[address]

    # 캐시에 없으면 API 호출 진행
    url = 'https://dapi.kakao.com/v2/local/search/keyword.json'
    headers = {'Authorization': f'KakaoAK {KAKAO_API_KEY}'}

    try:    
        query = f"{address}"
        result = await fetch_post(session, url, data={'query': query}, headers=headers)
        data = json.loads(result)
        place_name, lat, lng = None, None, None
        
        if data['documents'] and len(data['documents']) > 0:
            documents = [x for x in data['documents'] if '아파트' in x['category_name']]
            if len(documents) == 0:
                documents = [x for x in data['documents'] if '주거시설' in x['category_name']]

            if len(documents) > 0:
                doc = documents[0]
                place_name = doc['place_name'] if 'place_name' in doc else None
                lat = doc['y']
                lng = doc['x']

            else:
                documents = data['documents']
                doc = documents[0]
                lat = doc['y']
                lng = doc['x']
        # 2. 캐시에 저장
        result_tuple = (place_name, lat, lng)
        ADDRESS_CACHE[address] = result_tuple
                
        return result_tuple

    except Exception as e:
        print(f"KAKAO ERROR:{str(e)}")
    
    return None, None, None



# ------------------------------------------------- #

async def main():
    # [NEW] 시작 시 기존 캐시 파일 로드
    global ADDRESS_CACHE
    if os.path.exists(CACHE_FILE_PATH):
        try:
            with open(CACHE_FILE_PATH, 'r', encoding='utf-8') as f:
                ADDRESS_CACHE = json.load(f)
            print(f"기존 캐시 로드 완료: {len(ADDRESS_CACHE)}개 주소")
        except Exception as e:
            print(f"캐시 파일 로드 실패 (새로 시작): {e}")
            ADDRESS_CACHE = {}
    else:
        print("기존 캐시 파일 없음. 새로 시작합니다.")

    LAWD_CD = [{'code': '11110', 'kor_name': '서울특별시 종로구'}, {'code': '11140', 'kor_name': '서울특별시 중구'}, {'code': '11170', 'kor_name': '서울특별시 용산구'}, {'code': '11200', 'kor_name': '서울특별시 성동구'}, {'code': '11215', 'kor_name': '서울특별시 광진구'}, {'code': '11230', 'kor_name': '서울특별시 동대문구'}, {'code': '11260', 'kor_name': '서울특별시 중랑구'}, {'code': '11290', 'kor_name': '서울특별시 성북구'}, {'code': '11305', 'kor_name': '서울특별시 강북구'}, {'code': '11320', 'kor_name': '서울특별시 도봉구'}, {'code': '11350', 'kor_name': '서울특별시 노원구'}, {'code': '11380', 'kor_name': '서울특별시 은평구'}, {'code': '11410', 'kor_name': '서울특별시 서대문구'}, {'code': '11440', 'kor_name': '서울특별시 마포구'}, {'code': '11470', 'kor_name': '서울특별시 양천구'}, {'code': '11500', 'kor_name': '서울특별시 강서구'}, {'code': '11530', 'kor_name': '서울특별시 구로구'}, {'code': '11545', 'kor_name': '서울특별시 금천구'}, {'code': '11560', 'kor_name': '서울특별시 영등포구'}, {'code': '11590', 'kor_name': '서울특별시 동작구'}, {'code': '11620', 'kor_name': '서울특별시 관악구'}, {'code': '11650', 'kor_name': '서울특별시 서초구'}, {'code': '11680', 'kor_name': '서울특별시 강남구'}, {'code': '11710', 'kor_name': '서울특별시 송파구'}, {'code': '11740', 'kor_name': '서울특별시 강동구'}, {'code': '41110', 'kor_name': '경기도 수원시'}, {'code': '41111', 'kor_name': '경기도 수원시 장안구'}, {'code': '41113', 'kor_name': '경기도 수원시 권선구'}, {'code': '41115', 'kor_name': '경기도 수원시 팔달구'}, {'code': '41117', 'kor_name': '경기도 수원시 영통구'}, {'code': '41130', 'kor_name': '경기도 성남시'}, {'code': '41131', 'kor_name': '경기도 성남시 수정구'}, {'code': '41133', 'kor_name': '경기도 성남시 중원구'}, {'code': '41135', 'kor_name': '경기도 성남시 분당구'}, {'code': '41150', 'kor_name': '경기도 의정부시'}, {'code': '41170', 'kor_name': '경기도 안양시'}, {'code': '41171', 'kor_name': '경기도 안양시 만안구'}, {'code': '41173', 'kor_name': '경기도 안양시 동안구'}, {'code': '41190', 'kor_name': '경기도 부천시'}, {'code': '41192', 'kor_name': '경기도 부천시 원미구'}, {'code': '41194', 'kor_name': '경기도 부천시 소사구'}, {'code': '41196', 'kor_name': '경기도 부천시 오정구'}, {'code': '41210', 'kor_name': '경기도 광명시'}, {'code': '41220', 'kor_name': '경기도 평택시'}, {'code': '41250', 'kor_name': '경기도 동두천시'}, {'code': '41270', 'kor_name': '경기도 안산시'}, {'code': '41271', 'kor_name': '경기도 안산시 상록구'}, {'code': '41273', 'kor_name': '경기도 안산시 단원구'}, {'code': '41280', 'kor_name': '경기도 고양시'}, {'code': '41281', 'kor_name': '경기도 고양시 덕양구'}, {'code': '41285', 'kor_name': '경기도 고양시 일산동구'}, {'code': '41287', 'kor_name': '경기도 고양시 일산서구'}, {'code': '41290', 'kor_name': '경기도 과천시'}, {'code': '41310', 'kor_name': '경기도 구리시'}, {'code': '41360', 'kor_name': '경기도 남양주시'}, {'code': '41370', 'kor_name': '경기도 오산시'}, {'code': '41390', 'kor_name': '경기도 시흥시'}, {'code': '41410', 'kor_name': '경기도 군포시'}, {'code': '41430', 'kor_name': '경기도 의왕시'}, {'code': '41450', 'kor_name': '경기도 하남시'}, {'code': '41460', 'kor_name': '경기도 용인시'}, {'code': '41461', 'kor_name': '경기도 용인시 처인구'}, {'code': '41463', 'kor_name': '경기도 용인시 기흥구'}, {'code': '41465', 'kor_name': '경기도 용인시 수지구'}, {'code': '41480', 'kor_name': '경기도 파주시'}, {'code': '41500', 'kor_name': '경기도 이천시'}, {'code': '41550', 'kor_name': '경기도 안성시'}, {'code': '41570', 'kor_name': '경기도 김포시'}, {'code': '41590', 'kor_name': '경기도 화성시'}, {'code': '41591', 'kor_name': '경기도 화성시 만세구'}, {'code': '41593', 'kor_name': '경기도 화성시 효행구'}, {'code': '41595', 'kor_name': '경기도 화성시 병점구'}, {'code': '41597', 'kor_name': '경기도 화성시 동탄구'}, {'code': '41610', 'kor_name': '경기도 광주시'}, {'code': '41630', 'kor_name': '경기도 양주시'}, {'code': '41650', 'kor_name': '경기도 포천시'}, {'code': '41670', 'kor_name': '경기도 여주시'}, {'code': '41800', 'kor_name': '경기도 연천군'}, {'code': '41820', 'kor_name': '경기도 가평군'}, {'code': '41830', 'kor_name': '경기도 양평군'}]

    async with aiohttp.ClientSession() as session:
        today = date.today()
        today_month_str = today.strftime("%Y%m")
        before_1_month = today.replace(day=1) - timedelta(days=1)
        before_1_month_str = before_1_month.strftime("%Y%m")
        before_2_month_str = (before_1_month.replace(day=1) - timedelta(days=1)).strftime("%Y%m")
        data = []
        
        api_call_count = 0
        cache_hit_count = 0

        for sggCd in LAWD_CD:
            result = []
            result.extend(await fetch_all_apt_trade_data_async(DATAGOKR_API_KEY, sggCd['code'], today_month_str))
            result.extend(await fetch_all_apt_trade_data_async(DATAGOKR_API_KEY, sggCd['code'], before_1_month_str))
            result.extend(await fetch_all_apt_trade_data_async(DATAGOKR_API_KEY, sggCd['code'], before_2_month_str))

            for x in result:
                x["ADDRESS"] = f"{sggCd['kor_name']} {x['umdNm']} {x['jibun']}"

                if x["ADDRESS"] in ADDRESS_CACHE:
                    cache_hit_count += 1
                else:
                    api_call_count += 1

                place_name, lat, lng = await get_lat_lon(session, x["ADDRESS"])
                if lat and lng:
                    data.append({"address": x["ADDRESS"], "place_name": x["aptNm"] if place_name is None else place_name, "aptNm": x["aptNm"], "lat": lat, "lng": lng, "date": f"{x['dealYear']}{x['dealMonth'].zfill(2)}{x['dealDay'].zfill(2)}", "sggCd": sggCd["code"], "excluUseAr": x["excluUseAr"], "dealingGbn":x["dealingGbn"], "floor":x["floor"], "dealAmount":x["dealAmount"]})

        print(f"작업 완료: API 호출 {api_call_count}회, 캐시 사용 {cache_hit_count}회")

        os.makedirs('data', exist_ok=True)
        last_updated = (datetime.now(timezone.utc) + timedelta(hours=9)).strftime("%Y-%m-%d %H:%M:%S")
        print(last_updated)

        # 1. 데이터 파일 저장
        output = {"last_updated": last_updated, "data": data}
        with open('data/data.json', 'w', encoding='utf-8') as f:
            json.dump(output, f, ensure_ascii=False, indent=2)

        # [NEW] 2. 업데이트된 캐시 파일 저장 (다음 실행을 위해)
        with open(CACHE_FILE_PATH, 'w', encoding='utf-8') as f:
            json.dump(ADDRESS_CACHE, f, ensure_ascii=False, indent=2)
            print(f"캐시 파일 저장 완료: {CACHE_FILE_PATH}")

# -------------------- 실행 -------------------- #
if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
