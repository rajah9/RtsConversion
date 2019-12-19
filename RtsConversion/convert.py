"""
convert several files from IBID to LightSpeed.

Interesting Python features:
* Reads Utilities from a relative path.
* Does a list comprehension from a dictionary on a condition in clean_inventory
"""
from os.path import realpath, abspath, split, join
from inspect import currentframe, getfile
import sys
cmd_folder = realpath(abspath(split(getfile(currentframe() ))[0]))
if cmd_folder not in sys.path:
    sys.path.insert(0, cmd_folder)
# cmd_subfolder = realpath(
#     abspath(join(split(getfile(currentframe()))[0], "Utilities")))
# if cmd_subfolder not in sys.path:
#     sys.path.insert(0, cmd_subfolder)
import pandas as pd
from ApplicationUtil import ApplicationUtil
from StringUtil import StringUtil
from PandasUtil import DataFrameSplit

class RtsConvert(ApplicationUtil):
    def __init__(self, yaml_file:str):
        super().__init__(yaml_file)
        self.su = StringUtil()

    def load_vendors(self):
        vendors_file = self._d.asnamedtuple.inputVendorFile
        self.logger.debug(f'Reading vendors file: {vendors_file}')
        if self.fu.file_exists(vendors_file):
            self._df_cust = self.pu.read_df_from_excel(excelFileName=vendors_file, excelWorksheet='Sheet1', header=0)
            self.pu.get_rowCount_colCount(self._df_cust)
            cols = self.pu.get_df_headers(self._df_cust)
            self._df_cust = self.pu.coerce_to_string(self._df_cust, cols)
            self.pu.replace_vals(df=self._df_cust, replace_me='nan', new_val='')
            self.logger.debug(f'Header is: {self._df_cust.head()}')

    def f_clean_phone(self, orig_phone:str) -> str:
        ans = self.su.parse_phone(orig_phone, should_remove_blanks=True)
        return ans

    def clean_vendors(self):
        # Clean phone numbers
        self._df_cust['Phone'] = self._df_cust['Phone'].apply(self.f_clean_phone)
        self._df_cust['Fax'] = self._df_cust['Fax'].apply(self.f_clean_phone)
        # TODO: Address foreign numbers, like Germany.
        # Clean zip codes (remove trailing -)
        self._df_cust['Zip'].replace(to_replace=r'-$', value='', regex=True, inplace=True)
        # TODO: Remove country zips

        # Remap column names (according to dictionary in yaml file)
        vendor_col_map = self._d.asnamedtuple.vendorMapping
        self.logger.debug(f'Read in vendor mapping: {vendor_col_map}')
        self.pu.replace_col_names(df=self._df_cust, replace_dict=vendor_col_map, is_in_place=True)
        self.logger.debug(f'Lightspeed column names: {self._df_cust.head()}')

    def write_vendors(self):
        vendors_output_file = self._d.asnamedtuple.outputVendorFile
        self.logger.debug(f'Writing vendors file: {vendors_output_file}')
        self.pu.write_df_to_excel(df=self._df_cust, excelFileName=vendors_output_file, excelWorksheet='RTS Vendors')

    def load_inventory(self):
        inventory_file = self._d.asnamedtuple.inputInventoryFile
        self.logger.debug(f'Reading inventory file: {inventory_file}')
        if self.fu.file_exists(inventory_file):
            self._df_inv = self.pu.read_df_from_excel(excelFileName=inventory_file, excelWorksheet='Sheet1', header=0)
            self.pu.get_rowCount_colCount(self._df_inv)
            cols = self.pu.get_df_headers(self._df_inv)
            self._df_inv = self.pu.coerce_to_string(self._df_inv, cols)
            self.pu.replace_vals(df=self._df_inv, replace_me='nan', new_val='')
            self.logger.debug(f'Header is: {self._df_inv.head()}')
        else:
            self.logger.error(f'Inventory file {inventory_file} was not found. Exiting.')
            exit(-1)

    def f_clean_discount(self, orig_discount:str) -> str:
        ans = 'No' if orig_discount[:4].lower() == "no d" else ''
        return ans

    def f_clean_custom_sku(self, sku:str) -> str:
        """
        Replace the EANs of 13 characters in length with blanks.
        :param sku:
        :return:
        """
        if len(sku) == 13:
            return ''
        return sku

    def f_clean_item_code(self, sku:str) -> str:
        """
        Replace those item codes that are not 13 characters in length with blanks.
        :param sku:
        :return:
        """
        if len(sku) == 13:
            return sku
        return ''

    def f_truncate(self, field:str, max:int=255) -> str:
        """
        truncate the given field to the max length
        :param field: field to truncate
        :param max: length to truncate to
        :return:
        """
        if len(field) < max:
            return field
        self.logger.warning(f'Truncating field: {field} to {max} characters.')
        return field[:max]

    def f_fix_quantity(self, qty:int) -> int:
        """
        Replace negative quantities with 0.
        :param qty:
        :return:
        """
        if qty >= 0:
            return qty
        return 0

    def is_added_by_ibid(self, strs:list):
        ans = [x.startswith("--Added By Ibid") for x in strs]
        return ans

    def is_food_code(self, cats:list):
        su = StringUtil()
        ans = [x.lower() in ['ofoo', 'snac', 'coff'] for x in cats]
        return ans

    def is_misc_sku(self, skus:list):
        ans = [x.lower().startswith("x") for x in skus]
        return ans

    def is_missing_sell(self, prices:list):
        ans = [x <= 0 for x in prices]
        return ans

    def f_fix_sell(self, x):
        if x['SellPrice'] > 0:
            return x['SellPrice']
        else:
            return x['ListPrice']

    def clean_inventory(self):
        inventory_col_map = self._d.asnamedtuple.inventoryMapping
        self.logger.debug(f'Read in inventory mapping: {inventory_col_map}')

        su = StringUtil()

        str_cols = self.pu.get_df_headers(self._df_inv)
        num_cols = []
        str_cols.remove('ListPrice')
        num_cols.append('ListPrice')
        str_cols.remove('CostPrice')
        num_cols.append('CostPrice')
        str_cols.remove('OnHand')
        num_cols.append('OnHand')
        str_cols.remove('SellPrice')
        num_cols.append('SellPrice')

        self._df_inv = self.pu.coerce_to_string(self._df_inv, str_cols)
        self._df_inv = self.pu.coerce_to_numeric(self._df_inv, num_cols)

        # Ensure quantity is non-negative.
        self._df_inv = self.pu.replace_col_using_func(df=self._df_inv, column='OnHand', func=self.f_fix_quantity)

        # self._df_inv['Misc1'] = self._df_inv['Misc1'].apply(self.f_clean_discount)
        self._df_inv = self.pu.replace_col_using_func(df=self._df_inv, column='Misc1', func=self.f_clean_discount)

        # Create a new (Lightspeed) column "Custom SKU" for the ItemCode entries that are not 13 chars long
        new_col = 'Custom SKU'
        self._df_inv[new_col] = self._df_inv['ItemCode']
        self._df_inv = self.pu.replace_col_using_func(df=self._df_inv, column=new_col, func=self.f_clean_custom_sku)

        # Replace ItemCode entries that are not 13 chars long with blanks
        self._df_inv = self.pu.replace_col_using_func(df=self._df_inv, column='ItemCode', func=self.f_clean_item_code)

        # Force title to mixed case
        title_capitalization = self._d.asnamedtuple.titleCapitalization
        if title_capitalization == "title":
            self._df_inv = self.pu.replace_col_using_func(df=self._df_inv, column='Title', func=su.capitalize_as_title)

        author_capitalization = self.yaml_entry('authorCapitalization')
        if author_capitalization == "title":
            self._df_inv = self.pu.replace_col_using_func(df=self._df_inv, column='Author', func=su.capitalize_as_title)
        elif author_capitalization == "allcaps":
            self._df_inv = self.pu.replace_col_using_func(df=self._df_inv, column='Author', func=su.all_caps)

        # Append Author to Title
        auth_sep = ' -- '
        self._df_inv['Title'] = self._df_inv['Title'] + auth_sep + self._df_inv['Author']

        # Truncate Title to 255 chars
        self._df_inv = self.pu.replace_col_using_func(df=self._df_inv, column='Title', func=su.truncate)
        # TODO: Run Fix to eliminate over UTF-8

        # Remove Added By Ibid in Description
        mask_ibid = self.pu.mark_rows(self._df_inv, 'Title', self.is_added_by_ibid)

        # Remove food codes OFOO, SNAC, and COFF
        mask_food = self.pu.mark_rows(self._df_inv, 'Cat1', self.is_food_code)

        # Remove 'Custom SKU' that start with x or X
        mask_misc = self.pu.mark_rows(self._df_inv, new_col, self.is_misc_sku)

        masks = [mask_ibid[i] | mask_food[i] | mask_misc[i] for i in range(len(mask_food))]

        df_deleted = self.pu.masked_df(self._df_inv, masks)
        self.logger.debug(f'df_deleted header: {df_deleted.head()}')
        self.write_deleted(df_deleted)

        self._df_inv = self.pu.masked_df(self._df_inv, masks, invert_mask=True)

        # Round prices to 2 decimal places
        self._df_inv = self.pu.round(self._df_inv, {'ListPrice': 2, 'CostPrice': 2, 'SellPrice': 2})

        # TODO: If SellPrice <= 0, copy ListPrice over it.
        self._df_inv['SellPrice'] = self._df_inv.apply(self.f_fix_sell, axis=1)

        # Delete unused columns with XX
        cols_to_drop = [x for x, value in inventory_col_map.items() if value=="XX"]
        self.logger.debug(f'about to drop columns: {cols_to_drop}')
        self.pu.drop_col(df=self._df_inv, columns=cols_to_drop, is_in_place=True)
        self.pu.replace_col_names(df=self._df_inv, replace_dict=inventory_col_map, is_in_place=True)
        self.logger.debug(f'Lightspeed column names: {self._df_inv.head()}')

    def write_inventory(self):
        su = StringUtil()
        inventory_output_file = self._d.asnamedtuple.outputInventoryFile
        self.logger.debug(f'Writing inventory file: {inventory_output_file}')
        max_rows = self._d.asnamedtuple.maxLines
        # self.pu.write_df_to_excel(df=self._df_inv, excelFileName=inventory_output_file, excelWorksheet='RTS Vendors')
        little_dfs = DataFrameSplit(my_df=self._df_inv, interval=max_rows)
        combined_sizes = 0
        for i, df in enumerate(little_dfs):
            self.logger.debug(f'Set {i}: {len(df)}')
            combined_sizes += len(df)
            two_digit_file_number = su.leading_2_places(i)
            split_fn = su.replace_first(old='##', new=two_digit_file_number, myString=inventory_output_file)
            self.pu.write_df_to_excel(df=df, excelFileName=split_fn, excelWorksheet='RTS Inventory')

    def load_customers(self):
        self._df_cust = self.load_df_from_excel(input_file_yaml_entry='inputCustomerFile', worksheet='Sheet1')
        if self.fu.file_exists(customers_file):
            self._df_cust = self.pu.read_df_from_excel(excelFileName=customers_file, excelWorksheet='Sheet1', header=0)
            self.pu.get_rowCount_colCount(self._df_cust)
            cols = self.pu.get_df_headers(self._df_cust)
            self._df_cust = self.pu.coerce_to_string(self._df_cust, cols)
            self.pu.replace_vals(df=self._df_cust, replace_me='nan', new_val='')
            self.logger.debug(f'Header is: {self._df_cust.head()}')

    def clean_customers(self):
        # Remove duplicates
        self._df_cust = self.pu.drop_duplicates(df=self._df_cust, fieldList=['Account'], keep='last')

        # Clean phone numbers
        self._df_cust['Phone 1'] = self._df_cust['Phone 1'].apply(self.f_clean_phone)
        self._df_cust['Phone2'] = self._df_cust['Phone2'].apply(self.f_clean_phone)
        self._df_cust['Fax'] = self._df_cust['Fax'].apply(self.f_clean_phone)
        # TODO: Address foreign numbers, like Germany.
        # Clean zip codes (remove trailing -)
        self._df_cust['Zip'].replace(to_replace=r'-$', value='', regex=True, inplace=True)
        # TODO: Remove country zips

        # Remap column names (according to dictionary in yaml file)
        customer_col_map = self._d.asnamedtuple.customerMapping
        self.logger.debug(f'Read in customer mapping: {customer_col_map}')

        # Delete unused columns with XX
        cols_to_drop = [x for x, value in customer_col_map.items() if value=="XX"]
        self.logger.debug(f'about to drop columns: {cols_to_drop}')
        self.pu.drop_col(df=self._df_cust, columns=cols_to_drop, is_in_place=True)

        self.pu.replace_col_names(df=self._df_cust, replace_dict=customer_col_map, is_in_place=True)
        self.logger.debug(f'Lightspeed column names: {self._df_cust.head()}')

    def write_customers(self):
        self.write_excel(self._df_cust, 'outputCustomerFile', 'Customers')

    def write_deleted(self, df:pd.DataFrame) -> None:
        self.write_excel(df, 'outputSuspenseFile', 'Suspense')

rc = RtsConvert(r'C:\Users\Owner\PycharmProjects\RtsConversion\convert.yaml')
rc.load_customers()
rc.clean_customers()
rc.write_customers()

# rc.load_vendors()
# rc.clean_vendors()
# rc.write_vendors()

# rc.load_inventory()
# rc.clean_inventory()
# rc.write_inventory()
