import os 
import pickle
from collections import defaultdict


class GetTimestampData():
    """
    Extract locality of the UFS datasets of interest & generate a dictionary which will
    map the UFS dataset files into the following dataset types:
    Input data, WW3 input data, Baseline data, and BMIC data. 
    
    """
    
    def __init__(self, hpc_dir, avoid_fldrs, tracker_log_file="./data_from_ts_tracker/latest_rt.sh.pk"):
        """
        Args: 
            hpc_dir (str): Root directory path of where all the UFS timestamp datasets reside.
            avoid_fldrs (str): Foldername to ignore within main directory of interest on-prem.
                               Note: Some data folders were found w/ people's names within
                               them -- to be ignored.
            tracker_log_file (str): The folder directory containing the return of the UFS data 
                                    tracker bot.
        """
        
        # Datasets' main directory of interest. 
        self.hpc_dir = hpc_dir
        
        # Extract all data directories residing w/in datasets' main hpc directory.
        # Remove file directories comprise of a folder name.
        self.avoid_fldrs = avoid_fldrs
        self.file_dirs = self.get_data_dirs()
        
        # List of all data file directories w/in the UFS datasets.
        self.partition_datasets = self.get_input_bl_data()
        
        # Read timestamps recorded by the UFS tracker bot.
        self.tracker_log_file = tracker_log_file
        with open(self.tracker_log_file, 'rb') as log_file:
            self.data_log_dict = pickle.load(log_file)
        
        # Filter data directory paths to timestamps recorded by the UFS data tracker bot.
        # For bot, refer to https://github.com/NOAA-EPIC/ufs-dev_data_timestamps.
        self.filter2tracker_ts_datasets = self.get_tracker_ts_files()
        
        # Data files pertaining to specific timestamps of interest.
        # Select timestamp dataset(s) to transfer from RDHPCS on-disk to cloud
        #self.filter2specific_ts_datasets = self.get_specific_ts_files()
        
        # List of all data folders/files in datasets' main directory of interest.
        self.rt_root_list = os.listdir(self.hpc_dir)
        print("\033[1m" +\
              f"All Primary Dataset Folders & Files In Main Directory ({self.hpc_dir}):" +\
              f"\n\n\033[0m{self.rt_root_list}")
        
    def get_data_dirs(self):
        """
        Extract list of all file directories in datasets' main directory.
        
        Args:
            None
            
        Return (list): List of all file directories in datasets' main directory
        of interest.
        
        """
        
        # Generate list of all file directories residing w/in datasets' 
        # main directory of interest. 
        file_dirs = []
        file_size =[]
        for root_dir, subfolders, filenames in os.walk(self.hpc_dir):
            for file in filenames:
                file_dirs.append(os.path.join(root_dir, file))
        
        # Removal of personal names.
        if self.avoid_fldrs != None:
            file_dirs = [x for x in file_dirs if any(x for name in self.avoid_fldrs if name not in x)]
        
        return file_dirs

    def get_input_bl_data(self):
        """
        Extract list of all input file & baseline file directories.

        Args: 
            None
            
        Return (dict): Dictionary partitioning the file directories into the
        dataset types.
        
        *Note: Will keep 'INPUTDATA_ROOT_WW3' as a key wihtin the mapped dictionary
        -- in case, the NOAA development team decides to migrate WW3_input_data_YYYYMMDD
        out of the input-data-YYYYMMDD folder then, we will need to track the 
        'INPUTDATA_ROOT_WW3' related data files.

        """
        
        # Extract list of all input file & baseline file directories.
        partition_datasets = defaultdict(list) 
        for file_dir in self.file_dirs:

            # Input data files w/ root directory truncated.
            if any(subfolder in file_dir for subfolder in ['input-data', 'INPUT-DATA']):
                partition_datasets['INPUTDATA_ROOT'].append(file_dir.replace(self.hpc_dir, ""))

            # Baseline data files w/ root directory truncated.
            if any(subfolder in file_dir for subfolder in ['develop', 'ufs-public-release', 'DEVELOP', 'UFS-PUBLIC-RELEASE']):
                partition_datasets['BL_DATE'].append(file_dir.replace(self.hpc_dir, ""))
                
            # WW3 input data files w/ root directory truncated.
            if any(subfolder in file_dir for subfolder in ['WW3_input_data', 'ww3_input_data', 'WW3_INPUT_DATA']):
                partition_datasets['INPUTDATA_ROOT_WW3'].append(file_dir.replace(self.hpc_dir, ""))
                
            # BM IC input data files w/ root directory truncated.
            if any(subfolder in file_dir for subfolder in ['BM_IC', 'bm_ic']):
                partition_datasets['INPUTDATA_ROOT_BMIC'].append(file_dir.replace(self.hpc_dir, ""))


        return partition_datasets    
    
    def get_tracker_ts_files(self):
        """
        Filters file directory paths related to timestamps obtained from UFS data tracker bot.
        
        Args: 
            None

        Return (dict): Dictionary partitioning file directories into the
        timestamps of interest obtained from UFS data tracker bot.
        
        """
        
        # Reference timestamps captured from data tracker.
        filter2tracker_ts_datasets = defaultdict(list) 
        for dataset_type, timestamps in self.data_log_dict.items():
            
            # Extracts datafiles within the timestamps captured from data tracker.
            if dataset_type == 'INPUTDATA_ROOT':
                for subfolder in self.partition_datasets[dataset_type]:
                    if any(ts in subfolder for ts in timestamps):
                        filter2tracker_ts_datasets[dataset_type].append(subfolder)

            if dataset_type == 'BL_DATE':
                for subfolder in self.partition_datasets[dataset_type]:
                    if any(ts in subfolder for ts in timestamps):
                        filter2tracker_ts_datasets[dataset_type].append(subfolder)

            if dataset_type == 'INPUTDATA_ROOT_WW3':
                for subfolder in self.partition_datasets[dataset_type]:
                    if any(ts in subfolder for ts in timestamps):
                        filter2tracker_ts_datasets[dataset_type].append(subfolder)

            if dataset_type == 'INPUTDATA_ROOT_BMIC':
                for subfolder in self.partition_datasets[dataset_type]:
                    if any(ts in subfolder for ts in timestamps):
                        filter2tracker_ts_datasets[dataset_type].append(subfolder)
                        
        return filter2tracker_ts_datasets
    
    def get_specific_ts_files(self, input_ts, bl_ts, ww3_input_ts, bmic_ts):
        """
        Filters directory paths to timestamps of interest.
        
        Args: 
            input_ts (list): List of input timestamps to upload to cloud.
            bl_ts (list): List of baseline timestamps to upload to cloud.
            ww3_input_ts (list): List of WW3 input timestamps to upload to cloud.
            bmic_ts (list): List of BMIC timestamps to upload to cloud.
                                  
        Return (dict): Dictionary partitioning the file directories into the
        timestamps of interest specified by user.
        
        """
        
        # Create dictionary mapping the user's request of timestamps.
        specific_ts_dict = defaultdict(list)
        specific_ts_dict['INPUTDATA_ROOT'] = input_ts
        specific_ts_dict['BL_DATE'] = bl_ts
        specific_ts_dict['INPUTDATA_ROOT_WW3'] = ww3_input_ts
        specific_ts_dict['INPUTDATA_ROOT_BMIC'] = bmic_ts
        
        # Filter to directory paths of the timestamps specified by user.
        filter2specific_ts_datasets = defaultdict(list) 
        for dataset_type, timestamps in specific_ts_dict.items():
            
            # Extracts data files within the timestamps captured from data tracker.
            if dataset_type == 'INPUTDATA_ROOT':
                for subfolder in self.partition_datasets[dataset_type]:
                    if any(ts in subfolder for ts in timestamps):
                        filter2specific_ts_datasets[dataset_type].append(subfolder)

            if dataset_type == 'BL_DATE':
                for subfolder in self.partition_datasets[dataset_type]:
                    if any(ts in subfolder for ts in timestamps):
                        filter2specific_ts_datasets[dataset_type].append(subfolder)

            if dataset_type == 'INPUTDATA_ROOT_WW3':
                for subfolder in self.partition_datasets[dataset_type]:
                    if any(ts in subfolder for ts in timestamps):
                        filter2specific_ts_datasets[dataset_type].append(subfolder)

            if dataset_type == 'INPUTDATA_ROOT_BMIC':
                for subfolder in self.partition_datasets[dataset_type]:
                    if any(ts in subfolder for ts in timestamps):
                        filter2specific_ts_datasets[dataset_type].append(subfolder)
                        
        return filter2specific_ts_datasets    
    
