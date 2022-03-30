from get_timestamp_data import GetTimestampData
from progress_bar import ProgressPercentage
from upload_data import UploadData


class TransferBotData():
    """
    Obtain directories for the datasets tracked by the data tracker bot.
    
    """
    def __init__(self, linked_home_dir, platform="orion"):
        """
        Args: 
             linked_home_dir (str): User directory linked to the RDHPCS' root
                                    data directory.
             platform (str): RDHPCS of where the datasets will be sourced.
        """
    
        # Establish locality of where the dataseta will be sourced.
        self.linked_home_dir = linked_home_dir

        if platform == "orion":
            self.orion_rt_data_dir = self.linked_home_dir + "/noaa/nems/emc.nemspara/RT/NEMSfv3gfs/"
        else:
            print("Select a different platform.")
            
        # Filter to data tracker bot's timestamps & extract their corresponding UFS data file directories.
        self.filter2tracker_ts_datasets = GetTimestampData(self.orion_rt_data_dir, None).filter2tracker_ts_datasets
    
        # Upload datasets tracked by data tracker bot.
        UploadData(self.orion_rt_data_dir, self.filter2tracker_ts_datasets, use_bucket='rt').upload_files2cloud()
        #print("Reach to upload")        
        #print(self.filter2tracker_ts_datasets.keys())
        #print(self.filter2tracker_ts_datasets)
        
        
if __name__ == '__main__': 
    
    # Obtain directories for the datasets tracked by the data tracker bot.
    TransferBotData(linked_home_dir="/home/schin/work", platform="orion")