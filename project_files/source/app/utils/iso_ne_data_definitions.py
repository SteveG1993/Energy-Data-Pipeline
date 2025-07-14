import pandas as pd

def get_iso_ne_locations():
    # Define the data as a dictionary
    data = {
        "Location ID": [4000, 4001, 4002, 4003, 4004, 4005, 4006, 4007, 4008],
        "Location Name": [
            "Hub",
            "Maine Load Zone",
            "New Hampshire Load Zone",
            "Vermont Load Zone",
            "Connecticut Load Zone",
            "Rhode Island Load Zone",
            "Southeast Massachusetts Load Zone",
            "Northeast Massachusetts Load Zone",
            "Western/Central Massachusetts Load Zone"
        ]
    }

    # Create the DataFrame
    df = pd.DataFrame(data)

    return df

