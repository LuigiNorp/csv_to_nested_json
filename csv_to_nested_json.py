import json
import pandas as pd

class  CsvToJson:
    def __init__(self) -> None:
        self.file_name=None
        self.encoding=None
        self.column_names=None


    def filter_data(self,filter_value,csv:pd.DataFrame)->pd.array:
        """Filts a CSV file based in a filter value

        Args:
            filter_value (_type_): This will be used to filt csv file.
            csv (pd.DataFrame): This is de csv file.

        Returns:
            pd.array: This is a pd.array with filtered data
        """
        return csv[csv[self.column_names[0]]==filter_value]

    def csv_to_dictionary(self,file_name:str,column_names:list,csv:pd.DataFrame,filtd_data:pd.array,encoding:str='utf-8')->dict:
        self.file_name=file_name
        self.column_names = column_names

        self.col_1_dict={}
        col_1_list=list(csv[self.column_names[1]].unique())
        filt_by_col0=csv[csv[self.column_names[0]]==filtd_data]
        for col_1_item in col_1_list:
            filt_by_col1=filt_by_col0[filt_by_col0[self.column_names[1]]==col_1_item]
            main_info={}
            try:
                # Product Name (str)
                main_info[self.column_names[2]]=(filt_by_col1[self.column_names[2]].values)[0]
                
                # Ingredients (str list)
                ingrs_splited=str((filt_by_col1[self.column_names[3]]).values[0]).split(',')
                ingrs=[]
                for ing in ingrs_splited:
                    ingrs.append(ing.strip().lower())
                main_info[self.column_names[3]]=ingrs
                
                # Prices (float)
                main_info[self.column_names[4]]=float((filt_by_col1[self.column_names[4]].values)[0])
                self.col_1_dict[col_1_item]=main_info
            except Exception as e:
                continue
        return self.col_1_dict

    def csv_to_dictionary_2nd(self, file_name:str, column_names:list,nesting:int=2,encoding:str='utf-8',sep:str=',')->dict:
        """Converts a csv file into a python dictionary with a double nested format (to the second <2nd>),
        here is an example of its structure:
        {
            "Chica": {
                "Árabe": {
                    "name": "Árabe (Chica)",
                    "ingredient": [
                        "carne de tacos árabes",
                        "cebolla",
                        "pimiento morrón"
                    ],
                    "price": 130.0
                },
                "Atún": {
                    "name": "Atún (Chica)",
                    "ingredient": [
                        "atún"
                    ],
                    "price": 110.0
                },
            "Familiar": {
                "Árabe": {
                    "name": "Árabe (Familiar)",
                    "ingredient": [
                        "carne de tacos árabes",
                        "cebolla",
                        "pimiento morrón"
                    ],
                    "price": 210.0
                },
                "Atún": {
                    "name": "Atún (Familiar)",
                    "ingredient": [
                        "atún"
                    ],
                    "price": 180.0
                }
            }     
        }

        It works by deafult with encoding utf-8.

        Args:
            column_names (list): This is a list with the column names of the csv file.

        Returns:
            dict: A dictionary file with a double nested format.
        """
        self.file_name=file_name
        self.column_names = column_names
        self.encoding=encoding
        self.sep=sep
        self.csv=pd.read_csv(
        # TODO: Improve the name system
        "./functions/"+self.file_name+'.csv',
        encoding=self.encoding,
        sep=self.sep,
        names=self.column_names)

        for i in range(len(self.column_names)):
            self.column_names.append(self.column_names[i])
        
        loop=2
        while loop >= nesting:
            col_0_dict={}
            col_0_list=list(self.csv[self.column_names[0]].unique())

            if loop == nesting:                
                for col_0_item in col_0_list:
                    col_0_dict[col_0_item]=self.filter_data(self.column_names[0],self.csv)
                    col_0_dict[col_0_item]=self.csv_to_dictionary(self.file_name,self.column_names,self.csv,col_0_item)
                col_0_dict.pop(self.column_names[0])    
                return col_0_dict
            else:
                for col_0_item in col_0_list:
                    col_0_dict[col_0_item]=self.filter_data(self.column_names[0],self.csv)
                col_0_dict.pop(self.column_names[0])
            loop -= 1

    def csv_to_json_2nd(self, file_name:str, column_names:str)->json:
        """Converts a python dictionary into a nested json file.

        It works by default with encoding UTF-8, indent thru 4, and it ensure ascii false

        Args:
            file_name (str): This is a directory with the file name.
            column_list (list): This is a list with the column names of the csv file.

        Returns:
            json: This is a json file with a two nested format.
        """
        self.file_name=file_name
        self.column_names=column_names
        # TODO: Improve the name system
        with open('./functions/'+self.file_name+'.json','w') as outfile:
            json.dump(self.csv_to_dictionary_2nd(file_name, column_names,2),outfile,indent=4,ensure_ascii=False)


# filename = "pizzas"
# column_list = ['size','flavor','name','ingredient','price']
# print(csv_to_dictionary_2nd(filename, column_list))

file_name:str = "pizzas"
column_list = ['size','flavor','name','ingredient','price']

converter = CsvToJson()
converter.csv_to_json_2nd(file_name, column_list)