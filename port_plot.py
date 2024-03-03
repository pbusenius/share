import pandas as pd
import plotly.express as px

pd.options.plotting.backend = "plotly"


PORT_OF_OF_INTEREST = 16
PORT_CALLS = "combined.csv"
SHIP_TYPE_DICT = {
    20: "Wing in ground (WIG)",
    21: "Wing in ground (WIG)",
    22: "Wing in ground (WIG)",
    23: "Wing in ground (WIG)",
    24: "Wing in ground (WIG)",
    25: "Wing in ground (WIG)",
    26: "Wing in ground (WIG)",
    27: "Wing in ground (WIG)",
    28: "Wing in ground (WIG)",
    29: "Wing in ground (WIG)",
    30: "Fishing",
    31: "Towing",
    32: "Towing",
    40: "High speed craft",
    41: "High speed craft",
    42: "High speed craft",
    43: "High speed craft",
    44: "High speed craft",
    45: "High speed craft",
    46: "High speed craft",
    47: "High speed craft",
    48: "High speed craft",
    49: "High speed craft",
    52:	"Tug",
    60: "Passenger",
    61: "Passenger",
    62: "Passenger",
    63: "Passenger",
    64: "Passenger",
    65: "Passenger",
    66: "Passenger",
    67: "Passenger",
    68: "Passenger",
    69: "Passenger",
    70: "Cargo",
    71: "Cargo",
    72: "Cargo",
    73: "Cargo",
    74: "Cargo",
    75: "Cargo",
    76: "Cargo",
    77: "Cargo",
    78: "Cargo",
    79: "Cargo",
    80: "Tanker",
    81: "Tanker",
    82: "Tanker",
    83: "Tanker",
    84: "Tanker",
    85: "Tanker",
    86: "Tanker",
    87: "Tanker",
    88: "Tanker",
    89: "Tanker"
}


def main():
    df = pd.read_csv(PORT_CALLS)

    df = df[df.port_id==PORT_OF_OF_INTEREST]

    df["TIMESTAMPUTC_PORTCALL"] = pd.to_datetime(df['TIMESTAMPUTC_PORTCALL'].astype(int), unit='ms')

    df = df.sort_values("TIMESTAMPUTC_PORTCALL")

    port_calls_df = df.groupby(pd.Grouper(key='TIMESTAMPUTC_PORTCALL', axis=0,  
                      freq='D'))["MMSI"].count().reset_index(name="vessel_count") 

    port_calls_df["moving_vessel_count"] = port_calls_df["vessel_count"].transform(lambda x: x.rolling(10, 1).mean())   
    
    fig = px.line(port_calls_df, x="TIMESTAMPUTC_PORTCALL", y="moving_vessel_count",
                  labels={
                     "TIMESTAMPUTC_PORTCALL": "Date",
                     "moving_vessel_count": "Port Calls",
                 },
                title="Port Calls - Rotterdam (DDPI 16)")
    fig.show()

    ship_type_df = df.groupby("SHIPANDCARGOTYPECODE")["SHIPANDCARGOTYPECODE"].count().reset_index(name="count")

    ship_type_df = ship_type_df[ship_type_df["SHIPANDCARGOTYPECODE"] <= 99]
    ship_type_df = ship_type_df[ship_type_df["SHIPANDCARGOTYPECODE"] >= 1]
    ship_type_df = ship_type_df[ship_type_df["count"] >= 500]

    ship_type_df["SHIPANDCARGOTYPECODE"] = ship_type_df["SHIPANDCARGOTYPECODE"].map(SHIP_TYPE_DICT)

    ship_type_df["SHIPANDCARGOTYPECODE"] = ship_type_df["SHIPANDCARGOTYPECODE"].fillna("Other")

    print(ship_type_df)

    fig = px.pie(ship_type_df, values="count", names="SHIPANDCARGOTYPECODE", title="Shift Type - Rotterdam (DDPI 16)")
    fig.show()

if __name__ == "__main__":
    main()
