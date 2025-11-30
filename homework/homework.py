"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """
    import os
    import glob
    import pandas as pd

    patron = os.path.join("files", "input", "bank-marketing-campaing-*.csv.zip")
    rutas = sorted(glob.glob(patron))

    dataframes = []
    for ruta in rutas:
        df_parcial = pd.read_csv(ruta, compression="zip")  

        if df_parcial.columns[0].startswith("Unnamed") or df_parcial.columns[0] == "":
            df_parcial = df_parcial.iloc[:, 1:]
        dataframes.append(df_parcial)

    df = pd.concat(dataframes, ignore_index=True)

    if "client_id" not in df.columns:
        df["client_id"] = range(len(df))

    carpeta_output = os.path.join("files", "output")
    os.makedirs(carpeta_output, exist_ok=True)

    client_cols = [
        "client_id",
        "age",
        "job",
        "marital",
        "education",
        "credit_default",
        "mortgage",
    ]
    client = df[client_cols].copy()

    client["job"] = (
        client["job"]
        .astype(str)
        .str.replace(".", "", regex=False)
        .str.replace("-", "_", regex=False)
    )

    client["education"] = client["education"].astype(str).str.replace(
        ".", "_", regex=False
    )
    client["education"] = client["education"].replace("unknown", pd.NA)

    if client["credit_default"].dtype == "O":
        client["credit_default"] = (client["credit_default"] == "yes").astype(int)

    if client["mortgage"].dtype == "O":
        client["mortgage"] = (client["mortgage"] == "yes").astype(int)

    client = client[
        ["client_id", "age", "job", "marital", "education", "credit_default", "mortgage"]
    ]
    client.to_csv(os.path.join(carpeta_output, "client.csv"), index=False)

    campaign_cols = [
        "client_id",
        "number_contacts",
        "contact_duration",
        "previous_campaign_contacts",
        "previous_outcome",
        "campaign_outcome",
        "month",
        "day",
    ]
    campaign = df[campaign_cols].copy()

    if campaign["previous_outcome"].dtype == "O":
        campaign["previous_outcome"] = (
            campaign["previous_outcome"] == "success"
        ).astype(int)

    if campaign["campaign_outcome"].dtype == "O":
        campaign["campaign_outcome"] = (
            campaign["campaign_outcome"] == "yes"
        ).astype(int)

    month_map = {
        "jan": "01",
        "feb": "02",
        "mar": "03",
        "apr": "04",
        "may": "05",
        "jun": "06",
        "jul": "07",
        "aug": "08",
        "sep": "09",
        "oct": "10",
        "nov": "11",
        "dec": "12",
    }

    campaign["day"] = campaign["day"].astype(int).astype(str).str.zfill(2)
    campaign["month_num"] = campaign["month"].astype(str).str.lower().map(month_map)

    campaign["last_contact_date"] = (
        "2022-" + campaign["month_num"] + "-" + campaign["day"]
    )

    campaign = campaign[
        [
            "client_id",
            "number_contacts",
            "contact_duration",
            "previous_campaign_contacts",
            "previous_outcome",
            "campaign_outcome",
            "last_contact_date",
        ]
    ]

    campaign.to_csv(os.path.join(carpeta_output, "campaign.csv"), index=False)

    economics_cols = ["client_id", "cons_price_idx", "euribor_three_months"]
    economics = df[economics_cols].copy()

    economics.to_csv(os.path.join(carpeta_output, "economics.csv"), index=False)

    return


if __name__ == "__main__":
    clean_campaign_data()
