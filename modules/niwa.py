from modules.log import logger
from modules.settings import niwaEnabled, niwaAPIKey
from modules.settings import ERROR_FETCHING_DATA
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from collections import defaultdict, OrderedDict
from prettytable import PrettyTable
import requests

class Niwa:

    def __init__(self, api_key):
        self.api_key = api_key
        self.uv_api_url = "https://api.niwa.co.nz/uv/data"
        self.tide_api_url = "https://api.niwa.co.nz/tides/data"
        self.user_agent = "Meshing-Around Bot/1.0 (+https://meshing-around.com)"
        self.timezone = ZoneInfo("Pacific/Auckland") # Since NIWA data is NZ specific
        
        # Caches for data to minimize API calls
        self.cache_length_hours = 8
        self.cache_max_records = 150

        # Cache a tuple of (tide_data, timestamp, deviceID)
        self.tide_data_cache = []
        self.uv_data_cache = []
 
    def get_tide_data(self, lat, long, deviceID): 
        data = self.__retrieve_tide_data(lat, long, deviceID)
        if data == ERROR_FETCHING_DATA:
            return ERROR_FETCHING_DATA
        formatted_data = self.__format_tide_data(data)
        return formatted_data

    def __format_tide_data(self, tidedata): 
        table = PrettyTable()
        table.field_names = ["Date", "Time", "Height (m)"]

        output = f"\nNIWA Tide Data for location ({tidedata['metadata']['latitude']}, {tidedata['metadata']['longitude']}):\n"
        
        for value in tidedata['values']:
            # Parse ISO 8601 time and convert to local timezone
            dt = datetime.fromisoformat(value['time'])
            # Since this data is only for New Zealand, convert to Auckland timezone
            akl_dt = dt.astimezone(timezone.utc).astimezone(self.timezone)

            table.add_row([ 
                akl_dt.strftime("%Y-%m-%d"), 
                akl_dt.strftime("%H:%M"), 
                value['value']
            ])

        output += table.get_string()
        return output

    def __retrieve_tide_data(self, lat, long, deviceID): 

        # Check cache first
        cached_data = self.__check_tide_data_cache(deviceID)
        if cached_data:
            logger.debug("Using cached NIWA Tide data")
            return cached_data
        else: 
            tide_data = self.__query_tide_data(lat, long, deviceID)
            if tide_data != ERROR_FETCHING_DATA:
                self.__cache_tide_data(tide_data, deviceID)
            return tide_data

    def __query_tide_data(self, lat, long, deviceID): 

        params = {
            # NIWA API requires lat and long as integers
            # Meshtastic generally provides floats
            "lat": int(float(lat)),
            "long": int(float(long)), 
            "numberOfDays": 2, 
            "startDate": datetime.now().strftime("%Y-%m-%d"),
            "datum": "LAT"
            # Omitting interval to get just high and low tides
        }
        headers = {
            "User-Agent": self.user_agent, 
            "x-apikey": self.api_key
        }
        try:
            # Log the request body before sending
            logger.debug(f"NIWA Tide API Request Params: {params}")
            response = requests.get(self.tide_api_url, params=params, headers=headers)

            logger.debug(f"NIWA Tide API Response: {response.status_code} - {response.text}")
            response.raise_for_status()  # Raise an error for bad status codes
            self.__cache_tide_data(response.json(), deviceID)
            return response.json()
        
        except Exception as e:
            logger.error(f"Error fetching NIWA Tide data: {e}")
            return ERROR_FETCHING_DATA

    def __check_tide_data_cache(self, deviceID):
        current_time = datetime.now()
        for entry in self.tide_data_cache:
            tide_data, timestamp, cached_deviceID = entry
            if cached_deviceID == deviceID:
                elapsed_hours = (current_time - timestamp).total_seconds() / 3600
                if elapsed_hours < self.cache_length_hours:
                    return tide_data
        return None    

    def __cache_tide_data(self, tide_data, deviceID):
        self.tide_data_cache.append((tide_data, datetime.now(), deviceID))

        # Purge excessive cache entries
        if len(self.tide_data_cache) > self.cache_max_records:
            self.tide_data_cache = self.tide_data_cache[-self.cache_max_records:]

        # Purge old cache entries
        current_time = datetime.now()
        self.tide_data_cache = [entry for entry in self.tide_data_cache 
                                 if (current_time - entry[1]).total_seconds() < self.cache_length_hours * 3600]

        return True

    def get_uv_data(self, lat, long, deviceID): 
        data = self.__retrieve_uv_data(lat, long, deviceID)
        if data == ERROR_FETCHING_DATA:
            return ERROR_FETCHING_DATA
        formatted_data = self.__format_uv_data(data)
        return formatted_data
    
    def get_uv_risk_string(self, uv_index):
        if uv_index is None:
            return "No data"
        try:
            uv_index = float(uv_index)
        except ValueError:
            return "Invalid data"
        
        if uv_index < 3:
            return "Low"
        elif 3 <= uv_index < 6:
            return "Moderate"
        elif 6 <= uv_index < 8:
            return "High"
        elif 8 <= uv_index < 11:
            return "Very High"
        else:
            return "Extreme"

    def __format_uv_data(self, uvdata): 
        table = PrettyTable()
        #table.field_names = ["Time", "Clear Sky", "Clear Sky Risk", "Cloudy Sky", "Cloudy Sky Risk"]

        output = f"\nNIWA UV Forecast Data for location ({uvdata['coord']}):\n"
        
        times = []
        for value in uvdata['products'][0]['values']:
            # Parse ISO 8601 time and convert to local timezone
            dt = datetime.fromisoformat(value['time'])
            # Since this data is only for New Zealand, convert to Auckland timezone
            akl_dt = dt.astimezone(timezone.utc).astimezone(self.timezone)
            times.append(akl_dt.strftime("%Y-%m-%d %H:%M"))

        clear_sky_values = []
        clear_sky_risk = []
        for value in uvdata['products'][1]['values']: 
            clear_sky_values.append(value['value'])
            clear_sky_risk.append(self.get_uv_risk_string(value['value']))

        cloudy_sky_values = []
        cloud_sky_risk = []
        for value in uvdata['products'][0]['values']:
            cloudy_sky_values.append(value['value'])
            cloud_sky_risk.append(self.get_uv_risk_string(value['value']))

        table.add_column(f"Time {self.timezone}", times)
        table.add_column("Clear Sky", clear_sky_values)
        table.add_column("Clear Sky Risk", clear_sky_risk)
        table.add_column("Cloudy Sky", cloudy_sky_values)
        table.add_column("Cloudy Sky Risk", cloud_sky_risk)

        output += table.get_string()
        return output

    def __query_uv_data(self, lat, long, deviceID): 
        params = {
            # NIWA API requires lat and long as integers
            # Meshtastic generally provides floats
            "lat": int(float(lat)),
            "long": int(float(long))
        }
        headers = {
            "User-Agent": self.user_agent, 
            "x-apikey": self.api_key
        }
        try:
            # Log the request body before sending
            logger.debug(f"NIWA UV API Request Params: {params}")
            response = requests.get(self.uv_api_url, params=params, headers=headers)

            logger.debug(f"NIWA UV API Response: {response.status_code} - {response.text}")
            response.raise_for_status()  # Raise an error for bad status codes
            return response.json()
        
        except Exception as e:
            logger.error(f"Error fetching NIWA UV data: {e}")
            return ERROR_FETCHING_DATA

    def __cache_uv_data(self, uv_data, deviceID):
        self.uv_data_cache.append((uv_data, datetime.now(), deviceID))

        # Purge excessive cache entries
        if len(self.uv_data_cache) > self.cache_max_records:
            self.uv_data_cache = self.uv_data_cache[-self.cache_max_records:]

        # Purge old cache entries
        current_time = datetime.now()
        self.uv_data_cache = [entry for entry in self.uv_data_cache 
                                 if (current_time - entry[1]).total_seconds() < self.cache_length_hours * 3600]

        return True
    
    def __retrieve_uv_data(self, lat, long, deviceID):  

        # Check cache first
        cached_data = self.__check_uv_data_cache(deviceID)
        if cached_data:
            logger.debug("Using cached NIWA UV data")
            return cached_data
        else: 
            uv_data = self.__query_uv_data(lat, long, deviceID)
            if uv_data != ERROR_FETCHING_DATA:
                self.__cache_uv_data(uv_data, deviceID)
            return uv_data
        
    def __check_uv_data_cache(self, deviceID):
        current_time = datetime.now()
        for entry in self.uv_data_cache:
            uv_data, timestamp, cached_deviceID = entry
            if cached_deviceID == deviceID:
                elapsed_hours = (current_time - timestamp).total_seconds() / 3600
                if elapsed_hours < self.cache_length_hours:
                    return uv_data
        return None