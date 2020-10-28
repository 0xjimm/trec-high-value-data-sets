import urllib
import pandas as pd

from io import BytesIO
from zipfile import ZipFile
from sqlalchemy import create_engine
from datetime import timedelta, datetime

from prefect import task, Flow
from prefect.schedules import IntervalSchedule
from prefect.environments import LocalEnvironment


@task(max_retries=3, retry_delay=timedelta(minutes=1))
def extract():
    """
    Download and return a DataFrame of all Texas Brokers and Realtors
    """

    # browser headers
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36"
    }

    # request
    req = urllib.request.Request(
        "https://www.trec.texas.gov/sites/default/files/high-value-data-sets/trecfile.zip",
        headers=headers,
    )

    # connection
    con = urllib.request.urlopen(req)

    # open zipped download
    with ZipFile(BytesIO(con.read())) as myzip:
        thezip = myzip.open("trecfile.txt")

    # column names
    column_names = [
        "License_Type",
        "License_Number",
        "Full_Name",
        "Suffix",
        "License_Status",
        "Original_License_Date",
        "License Expiration_Date",
        "Education_Status",
        "MCE_Status",
        "Designated_Supervisor_Flag",
        "Phone_Number",
        "Email_Address",
        "Mailing_Address_Line_1",
        "Mailing_Address_Line_2",
        "Mailing_Address_Line_3",
        "Mailing_Address_City",
        "Mailing_Address_State_Code",
        "Mailing_Address_Zip_Code",
        "Mailing_Address_County_Code",
        "Physical_Address_Line_1",
        "Physical_Address_Line_2",
        "Physical_Address_Line_3",
        "Physical_Address_City",
        "Physical_Address_State_Code",
        "Physical_Address_Zip_Code",
        "Physical_Address_County_Code",
        "Related_License_Type",
        "Related_License_Number",
        "Related_License_Full_Name",
        "Related_License_Suffix",
        "Related_License_Start_Date",
        "Agency_Identifier",
    ]

    # read zipfile to DataFrame
    df = pd.read_csv(
        thezip,
        names=column_names,
        index_col=False,
        sep="\t",
        encoding="latin1",
        low_memory=False,
    )

    return df


@task
def transform(df_realtors):
    """
    From a DataFrame of all Brokers and Realtors, filter by Houston zipcodes.
    """

    houston_zips = [
        "77001",
        "77002",
        "77003",
        "77004",
        "77005",
        "77006",
        "77007",
        "77008",
        "77009",
        "77010",
        "77011",
        "77012",
        "77013",
        "77014",
        "77015",
        "77016",
        "77017",
        "77018",
        "77019",
        "77020",
        "77021",
        "77022",
        "77023",
        "77024",
        "77025",
        "77026",
        "77027",
        "77028",
        "77029",
        "77030",
        "77031",
        "77032",
        "77033",
        "77034",
        "77035",
        "77036",
        "77037",
        "77038",
        "77039",
        "77040",
        "77041",
        "77042",
        "77043",
        "77044",
        "77045",
        "77046",
        "77047",
        "77048",
        "77049",
        "77050",
        "77051",
        "77052",
        "77053",
        "77054",
        "77055",
        "77056",
        "77057",
        "77058",
        "77059",
        "77060",
        "77061",
        "77062",
        "77063",
        "77064",
        "77065",
        "77066",
        "77067",
        "77068",
        "77069",
        "77070",
        "77071",
        "77072",
        "77073",
        "77074",
        "77075",
        "77076",
        "77077",
        "77078",
        "77079",
        "77080",
        "77081",
        "77082",
        "77083",
        "77084",
        "77085",
        "77086",
        "77087",
        "77088",
        "77089",
        "77090",
        "77091",
        "77092",
        "77093",
        "77094",
        "77095",
        "77096",
        "77098",
        "77099",
        "77201",
        "77202",
        "77203",
        "77204",
        "77205",
        "77206",
        "77207",
        "77208",
        "77209",
        "77210",
        "77212",
        "77213",
        "77215",
        "77216",
        "77217",
        "77218",
        "77219",
        "77220",
        "77221",
        "77222",
        "77223",
        "77224",
        "77225",
        "77226",
        "77227",
        "77228",
        "77229",
        "77230",
        "77231",
        "77233",
        "77234",
        "77235",
        "77236",
        "77237",
        "77238",
        "77240",
        "77241",
        "77242",
        "77243",
        "77244",
        "77245",
        "77248",
        "77249",
        "77251",
        "77252",
        "77253",
        "77254",
        "77255",
        "77256",
        "77257",
        "77258",
        "77259",
        "77261",
        "77262",
        "77263",
        "77265",
        "77266",
        "77267",
        "77268",
        "77269",
        "77270",
        "77271",
        "77272",
        "77273",
        "77274",
        "77275",
        "77277",
        "77279",
        "77280",
        "77282",
        "77284",
        "77287",
        "77288",
        "77289",
        "77290",
        "77291",
        "77292",
        "77293",
        "77297",
        "77299",
    ]

    df_houston_realtors = df_realtors[
        df_realtors["Mailing_Address_Zip_Code"].isin(houston_zips)
    ]

    return df_houston_realtors


@task(max_retries=3, retry_delay=timedelta(minutes=1))
def load(df_houston_realtors):
    """
    Load DataFrame of Houston Brokers and Realtors into a Database.
    """

    engine = create_engine("sqlite:///houston_realtors.db", echo=True)
    sqlite_connection = engine.connect()
    sqlite_table = "Houston_Realtors"

    df_houston_realtors.to_sql(sqlite_table, sqlite_connection, if_exists="replace")

    sqlite_connection.close()

    pass


if __name__ == "__main__":

    # scheduled to run every 12 hours
    schedule = IntervalSchedule(
        start_date=datetime.utcnow() + timedelta(seconds=5),
        interval=timedelta(hours=12),
    )

    # define Prefect flow
    with Flow("TREC-Realtors", schedule=schedule) as flow:
        realtor_data = extract()
        houston_realtor_data = transform(realtor_data)
        load_to_database = load(houston_realtor_data)

    flow.environment = LocalEnvironment(labels=["trec-prod"])

    # register flow with Prefect Cloud
    flow.register(project_name="trec-high-value-data-sets")
