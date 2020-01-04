import numpy as np
import pandas as pd
import yaml
from optparse import OptionParser

class prepare_data(object):
    def __init__(self, params):
        self.params = params
        self.train_benef = pd.read_csv(params['info']['base_dir']+'/data/Train_Beneficiarydata-1542865627584.csv')
        self.train_inpatient = pd.read_csv(params['info']['base_dir']+'/data/Train_Inpatientdata-1542865627584.csv')
        self.train_outpatient = pd.read_csv(params['info']['base_dir']+'/data/Train_Outpatientdata-1542865627584.csv')
        self.train_label = pd.read_csv(params['info']['base_dir']+'/data/Train-1542865627584.csv')
        
    def join_table(self):
        train_inpatient_df = pd.merge(self.train_inpatient, self.train_benef, on = 'BeneID', how = 'left')
        train_inpatient_df = pd.merge(train_inpatient_df, self.train_label, on = 'Provider', how = 'left')
        
        train_outpatient_df = pd.merge(self.train_outpatient, self.train_benef, on = 'BeneID', how = 'left')
        train_outpatient_df = pd.merge(train_outpatient_df, self.train_label, on = 'Provider', how = 'left')
        return train_inpatient_df, train_outpatient_df
    
    def run(self):
        inpateint_df, outpatient_df = self.join_table()
        inpateint_df.to_csv(params['info']['base_dir']+'/data/processed_inpatient_df.csv', index = False)
        print('Written processed inpatient file to: {}'.format(params['info']['base_dir']+'/data/processed_inpatient_df.csv'))
        outpatient_df.to_csv(params['info']['base_dir']+'/data/processed_outpatient_df.csv', index = False)
        print('Written processed outpatient file to: {}'.format(params['info']['base_dir']+'/data/processed_outpatient_df.csv'))
        
if __name__ == '__main__':
    parser = OptionParser(usage="usage: python get_processed_data.py  configFile",
                          version="0.1")
    opts,args = parser.parse_args()
    if len(args) < 1:
        print('Config and/or output file missing...')
        exit(0)
    params = yaml.load(open(args[0],'r'))
    process_df = prepare_data(params)
    process_df.run()
