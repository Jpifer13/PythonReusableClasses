#!/usr/bin/env python3

"""

Description
-----------
This is a dataset class object

Key changes
-----------
01/27/2020: JP created

Author
-----------
Jake Pifer

"""

# Standard libraries
import os
import sys
import logging
import pandas
from pandas import DataFrame as df
from pandas import read_csv

__version__ = "Version 1.0"
__application__ = "IrisDatasetClass"

class PandasDatasetClass:
    """
    This is a dataset class object. 
    It is structured to be a general dataset class
    """

    def __init__(self, column_names, data_url=None, data_path=None, logger=None):
        """
        __init__ Constructs an instance of this class. 
        
        Arguments:
            column_names {[List]} -- [List of the column names you want to use]
        
        Keyword Arguments:
            data_url {[string]} -- [Url to the data] (default: {None})
            data_path {[string]} -- [Windows path to data] (default: {None})
        """
        # Initiate dataset 
        self._dataset = None
        self._data_url = None
        self._data_path = None

        # Initialize logger  
        self._logger = logger

        if self._logger:
            self._logger.info(f"PandasDatasetClass accessed.")
            self._logger.info("Loading dataset...")

        self.load_dataset(column_names, data_url=data_url, data_path=data_path)

    @property
    def dataset(self):
        """
        Get this database instance
        
        Returns:
            [DataFrame] -- [Returns instance of the dataset]
        """
        return self._dataset

    @property
    def data_url(self):
        """
        Get the url used to create this dataset
        
        Returns:
            [string] -- [The url where this dataset was retrieved]
        """
        return self._data_url

    @data_url.setter
    def set_data_url(self, new_url, update_current_dataset=None, column_names=None):
        """
        This method updates the data_url property and can update the entire dataset as well with the new url
        
        Arguments:
            new_url {[string]} -- [New url for this dataset]
        
        Keyword Arguments:
            update_current_dataset {[bool]} -- [If true updates the dataset with new url data] (default: {None})
            column_names {[list]} -- [list of new column names for this dataset] (default: {None})
        """
        self._data_url = new_url

        # Update this dataset with new data if told to
        if update_current_dataset and column_names:
            self.load_dataset(column_names, data_url=self._data_url)
            self._logger.info(f'Successfully updated dataset with data from: {self._data_url}')
        elif update_current_dataset and not column_names:
            self._logger.exception('Can not update! No column names were given for new dataset!')
        else:
            self._logger.info(f'Successfully updated dataset url: {self._data_url}')
    
    @property
    def data_path(self):
        """
        Get the path used to create this dataset
        
        Returns:
            [string] -- [The path where this dataset was retrieved]
        """
        return self.data_path

    @data_path.setter
    def set_data_path(self, new_path, update_current_dataset=None, column_names=None):
        """
        This method updates the data_path property and can update the entire dataset as well with the new path
        
        Arguments:
            data_path {[string]} -- [New path for this dataset]
        
        Keyword Arguments:
            update_current_dataset {[bool]} -- [If true updates the dataset with new url data] (default: {None})
            column_names {[list]} -- [list of new column names for this dataset] (default: {None})
        """
        self._data_path = new_path

        # Update this dataset with new data if told to
        if update_current_dataset and column_names:
            self.load_dataset(column_names, data_path=self.data_path)
            self._logger.info(f'Successfully updated dataset with data from: {self.data_path}')
        elif update_current_dataset and not column_names:
            self._logger.exception('Can not update! No column names were given for new dataset!')
        else:
            self._logger.info(f'Successfully updated dataset path: {self.data_path}')

    def load_dataset(self, column_names, data_url=None, data_path=None):
        """
        This method loads the dataset
        
        Arguments:
            column_names {[List]} -- [List of the column names you want to use]
        
        Keyword Arguments:
            data_url {[string]} -- [Url to the data] (default: {None})
            data_path {[string]} -- [Windows path to data] (default: {None})
        """

        if data_url:
            # Load dataset
            self._data_url = data_url
            self._column_names = column_names
            self._dataset = read_csv(self._data_url, names=self._column_names)
            self._logger.info(f'Dataset at location: {self._data_url} successfully loaded!')
        elif data_path:
            # Load dataset
            self._data_path = data_path
            self._column_names = column_names
            self._dataset = read_csv(self._data_path, names=self._column_names)
            self._logger.info(f'Dataset at location: {self._data_path} successfully loaded!')
        else:
            # No data url or path
            self._logger.exception(f'No path or url to data has been provided:\n data_path = {data_path}\n data_url = {data_url}')

    def __str__(self):
        """
        Prints out the object as a string
        
        Returns:
            [string] -- [type of object, (name)]
        """
        className = type(self).__name__
        return "{},({})".format(className, self._name)

