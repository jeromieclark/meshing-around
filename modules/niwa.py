from modules.log import logger
from modules.settings import niwaEnabled, niwaAPIKey
from modules.settings import ERROR_FETCHING_DATA
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import requests
class Niwa:

    def __init__(self, api_key):
        self.api_key = api_key
        self.uv_api_url = "https://api.niwa.co.nz/uv/data"
        self.tide_api_url = "https://api.niwa.co.nz/tides/data"
        self.user_agent = "Meshing-Around Bot/1.0 (+https://meshing-around.com)"
        self.timezone = ZoneInfo("Pacific/Auckland") # Since NIWA data is NZ specific
        self.uv_records_per_response = 4
        
        # Caches for data to minimize API calls
        self.cache_length_hours = 8
        self.cache_max_records = 150

        # Cache a tuple of (tide_data, timestamp, deviceID)
        self.uv_data_cache = []
        self.tide_data_cache = []

        # Since this output can be long, prompt user for "more"
        self.uv_sessions = []

    def __prune_uv_sessions(self):        
        for session in self.uv_sessions:
            if (datetime.now(timezone.utc) - session['last_access']).total_seconds() > self.cache_length_hours * 3600:
                self.uv_sessions.remove(session)
                continue

    def __get_uv_session(self, deviceID): 
        self.__prune_uv_sessions()
        for session in self.uv_sessions:
            if session['deviceID'] == deviceID:
                return session

        # Create and append a new session if none exists
        new_session = {
            'deviceID': deviceID,
            'last_access': datetime.now(timezone.utc),
            'begin': 0
        }
        self.uv_sessions.append(new_session)
        return new_session

    def __update_uv_session(self, deviceID, last_access, begin): 
        session = self.__get_uv_session(deviceID)
        if session:
            session['last_access'] = last_access
            session['begin'] = begin
        else:
            self.uv_sessions.append({
                'deviceID': deviceID,
                'last_access': last_access,
                'begin': begin # index for pagination
            })
        return True

    def get_tide_data(self, lat, long, deviceID): 
        data = self.__retrieve_tide_data(lat, long, deviceID)
        if data == ERROR_FETCHING_DATA:
            return ERROR_FETCHING_DATA
        formatted_data = self.__format_tide_data(data)
        return formatted_data

    def __format_tide_data(self, tidedata): 

        output = f"\nNIWA Tide Data for location ({tidedata['metadata']['latitude']}, {tidedata['metadata']['longitude']}):\n"
        
        for value in tidedata['values']:
            # NIWA will send the forecast data in local time (UTC+19), so no timezone math required
            dt = datetime.fromisoformat(value['time'])
            output += f"{dt.strftime("%Y-%m-%d")} {dt.strftime("%H:%M")} {value['value']}m \n"

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
        session = self.__get_uv_session(deviceID)
        logger.debug(f"get_uv_data - session: str(session)")
        data = self.__retrieve_uv_data(lat, long, deviceID)
        if data == ERROR_FETCHING_DATA:
            return ERROR_FETCHING_DATA
        formatted_data = self.__format_uv_data(data, deviceID, session['begin'])
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
            return "Medium"
        elif 6 <= uv_index < 8:
            return "High"
        elif 8 <= uv_index < 11:
            return "Very High"
        else:
            return "Extreme"

    def __format_uv_data(self, uvdata, deviceID, begin=0): 
       
        output = f"\nNIWA UV Forecast Data for location ({uvdata['coord']}):\n"
        
        times = []
        for value in uvdata['products'][0]['values']:
            # Parse ISO 8601 time
            dt = datetime.fromisoformat(value['time'])
            times.append(dt.strftime("%d %b %H:%M"))

        clear_sky_values = []
        clear_sky_risk = []
        for value in uvdata['products'][1]['values']: 
            clear_sky_values.append('{0:3.1f}'.format(value['value']))
            clear_sky_risk.append(self.get_uv_risk_string(value['value']))

        cloudy_sky_values = []
        cloud_sky_risk = []
        for value in uvdata['products'][0]['values']:
            cloudy_sky_values.append(value['value'])
            cloud_sky_risk.append(self.get_uv_risk_string(value['value']))

        # combine lists into dictionary for ordered output
        uv_dict = []
        for i in range(len(times)):
            uv_dict.append({
                "time": times[i],
                "clear_sky_value": clear_sky_values[i],
                "clear_sky_risk": clear_sky_risk[i],
                "cloudy_sky_value": cloudy_sky_values[i],
                "cloudy_sky_risk": cloud_sky_risk[i]
            })
    
        end = begin + self.uv_records_per_response

        # If we've reached the end of the data, reset session to 0
        isEnd = False
        if end > len(uv_dict):
            end = len(uv_dict)
            self.__update_uv_session(deviceID, datetime.now(timezone.utc), 0)
            isEnd = True
        else: 
            self.__update_uv_session(deviceID, datetime.now(timezone.utc), end)

        for item in list(uv_dict)[begin:end]:
            output += f"{item['time']}\n"
            output += f"Clear Sky: {item['clear_sky_value']} ({item['clear_sky_risk']})\n"
            output += f"Cloudy Sky: {item['cloudy_sky_value']} ({item['cloudy_sky_risk']})\n\n"

        if isEnd == False:  
            output += "Show More?  Send \"nzuv\" to continue."
        else: 
            output += "Forecast complete."

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