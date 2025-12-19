"""
IAS Node Processor
Handles cadence calculation for IAS node (FR, IT, ES, MX)
"""

import pandas as pd
import numpy as np
from datetime import timedelta
import warnings
from .base_processor import BaseProcessor

warnings.filterwarnings('ignore')


class IASProcessor(BaseProcessor):
    """Processor for IAS node cadence calculations"""

    RISK_TO_CADENCE = {
        0: 30,
        1: 30,
        2: 60,
        3: 90,
        4: 180,
        5: 365
    }

    def __init__(self, status_callback=None):
        super().__init__(status_callback)
        self.node_name = "IAS"

        # IAS specific configurations
        self.IAS_Core = ['FR', 'IT', 'ES', 'ANY', 'Any']
        self.IAS_Cross = [
            'US', 'UK', 'GB', 'SA', 'AE', 'DE', 'SG', 'AU', 'IT', 'ES', 'FR', 'TR',
            'Any', 'ANY', 'BR', 'ZA', 'IN', 'NL', 'BE', 'QA', 'AT', 'MX', 'PL', 'SE', ''
        ]
        self.IAS_CrossListing_Sources = ['FR', 'IT', 'ES', 'MX']

    def get_required_files(self):
        """Return list of required files for IAS processor"""
        return [
            {
                'key': 'armt_file',
                'label': 'ARMT File',
                'description': 'ARMT_AGCL.xlsx',
                'help': 'Upload the ARMT AGCL Excel file'
            },
            {
                'key': 'master_file',
                'label': 'IAS Master Cadence File',
                'description': 'Previous Month IAS Cadence',
                'help': 'Upload the previous month\'s IAS cadence file'
            },
            {
                'key': 'outflow_file',
                'label': 'Outflow File',
                'description': 'Monthly Outflow Data',
                'help': 'Upload the outflow Excel file'
            }
        ]

    def get_node_config(self):
        """Return IAS node configuration"""
        return {
            'name': 'IAS',
            'full_name': 'IAS Node (FR/IT/ES)',
            'description': 'Process cadence for IAS node - France, Italy, Spain, Mexico',
            'color': '#28a745'
        }

    def process_armt(self, armt):
        """Process ARMT data for IAS"""
        self.log('📂 Processing ARMT data for IAS...')

        cols_to_drop = [
            'parent_job_status', 'parent_job_type', 'parent_last_updated_time',
            'child_job_status', 'child_job_type', 'child_last_updated_time'
        ]
        armt = armt.drop(cols_to_drop, axis=1, errors='ignore')

        armt['Source'] = armt['source_country'].astype(str).str.strip()
        armt['Destination'] = armt['include_destination_country'].astype(str).str.strip()

        # Process AmazonGlobal
        self.log('   Processing AmazonGlobal...')
        armt_core = armt[armt['program'] == 'AmazonGlobal'].copy()
        if len(armt_core) > 0:
            armt_core = armt_core.assign(Source=armt_core['Source'].str.split(',')).explode('Source')
            armt_core['Source'] = armt_core['Source'].str.strip()
            armt_core.loc[armt_core['Destination'].str.contains(',', na=False), 'Destination'] = 'SOME'

        # Process CrossListing
        self.log('   Processing CrossListing...')
        armt_cross = armt[armt['program'] == 'CrossListing'].copy()
        if len(armt_cross) > 0:
            armt_cross = armt_cross.assign(Destination=armt_cross['Destination'].str.split(',')).explode('Destination')
            armt_cross = armt_cross.assign(Source=armt_cross['Source'].str.split(',')).explode('Source')
            armt_cross['Source'] = armt_cross['Source'].str.strip()
            armt_cross['Destination'] = armt_cross['Destination'].str.strip()

        armt = pd.concat([armt_core, armt_cross], ignore_index=True)

        armt['ARC'] = armt['Source'] + '-' + armt['Destination']
        armt['combined_class'] = armt['parent_class'].astype(str) + "," + armt['child_class'].astype(str) + ',' + armt[
            'Source']
        armt['combined_class2'] = armt['parent_class'].astype(str) + "," + armt['child_class'].astype(str) + ',' + armt[
            'ARC']

        self.log(f'✅ ARMT processed: {len(armt):,} records')
        return armt

    def process_outflow(self, outflow):
        """Process outflow data for IAS"""
        self.log('📂 Processing Outflow data for IAS...')

        initial_count = len(outflow)

        outflow = outflow.dropna(subset=['root_cause', 'root_cause_details'])

        # IAS specific groups to remove
        del_groups = ['RP - AG Auditors', 'RP - AG Auditors CN', 'RP - AG Auditors PL']
        if 'assigned_to_group' in outflow.columns:
            outflow = outflow[~outflow['assigned_to_group'].isin(del_groups)]

        del_causes = ['Duplicate', 'Other', 'Negative Class']
        outflow = outflow[~outflow['root_cause'].isin(del_causes)]

        outflow['child_class'] = outflow['short_description'].astype(str).str.split(':').str[0].str.strip()
        outflow['parent_class'] = outflow['root_cause_details'].astype(str).str.split('\\\\').str[0].str.strip()
        outflow.loc[outflow['child_class'] == outflow['parent_class'], 'child_class'] = 'No-Child'

        outflow['Resolved date'] = pd.to_datetime(outflow['resolved_date'], errors='coerce')
        outflow['Resolved date'] = outflow['Resolved date'].apply(self.date_to_string)
        outflow = outflow.sort_values(by='Resolved date', ascending=False)

        outflow['resolution'] = outflow['resolution'].astype(str).str.split('\\\\').str[0]
        outflow.loc[outflow['child_class'] == outflow['parent_class'], 'child_class'] = 'No-Child'

        outflow['source'] = outflow['resolution'].str.split('-').str[0]
        outflow['combined_class'] = outflow['parent_class'].astype(str) + "," + outflow['child_class'].astype(
            str) + ',' + outflow['source']
        outflow['combined_class2'] = outflow['parent_class'].astype(str) + "," + outflow['child_class'].astype(
            str) + ',' + outflow['resolution']

        outflow['quantity'] = pd.to_numeric(outflow['quantity'], errors='coerce').fillna(0)
        outflow['vendor_id'] = pd.to_numeric(outflow['vendor_id'], errors='coerce').fillna(0).astype(int)
        outflow['NC'] = outflow['quantity'] + outflow['vendor_id']

        outflow1 = outflow.groupby('combined_class', as_index=False)['NC'].sum()
        outflow2 = outflow.groupby('combined_class2', as_index=False)['NC'].sum()

        self.log(f'✅ Outflow processed: {len(outflow):,} records (from {initial_count:,})')

        return outflow.reset_index(drop=True), outflow1, outflow2

    def create_nodes(self, armt):
        """Create source and destination mapping for IAS"""
        self.log('🗺️ Creating IAS node mappings...')

        nodes = set()

        # AmazonGlobal nodes - FR, IT, ES
        ag_mask = armt['program'] == 'AmazonGlobal'
        ag_data = armt[ag_mask]

        # FR nodes
        fr_mask = ag_data['Source'] == 'FR'
        nodes.update(ag_data.loc[fr_mask, 'combined_class'].tolist())

        # IT nodes
        it_mask = ag_data['Source'] == 'IT'
        nodes.update(ag_data.loc[it_mask, 'combined_class'].tolist())

        # ES nodes
        es_mask = ag_data['Source'] == 'ES'
        nodes.update(ag_data.loc[es_mask, 'combined_class'].tolist())

        # CrossListing nodes - sources FR, IT, ES, MX
        cl_mask = (
                (armt['program'] == 'CrossListing') &
                (armt['Source'].isin(self.IAS_CrossListing_Sources))
        )
        nodes.update(armt.loc[cl_mask, 'combined_class2'].tolist())

        self.log(f'✅ Created {len(nodes):,} unique IAS nodes')
        return list(nodes)

    def create_cadence(self, nodes, armt, outflow, outflow1, outflow2):
        """Create cadence dataframe for IAS"""
        self.log('⚙️ Creating IAS Cadence dataframe...')

        Cadence = pd.DataFrame({'Combined Classes': nodes})
        total = len(Cadence)
        self.log(f'   Processing {total:,} records...')

        self.log('   Building lookup tables...')

        armt_lookup1 = armt.drop_duplicates(subset=['combined_class']).set_index('combined_class')
        armt_lookup2 = armt.drop_duplicates(subset=['combined_class2']).set_index('combined_class2')

        outflow_lookup1 = outflow.drop_duplicates(subset=['combined_class']).set_index('combined_class')
        outflow_lookup2 = outflow.drop_duplicates(subset=['combined_class2']).set_index('combined_class2')

        nc_lookup1 = outflow1.set_index('combined_class')['NC'].to_dict()
        nc_lookup2 = outflow2.set_index('combined_class2')['NC'].to_dict()

        self.log('   Applying lookups...')

        cc = Cadence['Combined Classes']

        # Primary lookups
        Cadence['program'] = cc.map(armt_lookup1['program'].to_dict()).fillna(self.NOT_FOUND).astype(str)
        Cadence['Policies'] = cc.map(armt_lookup1['policy_name'].to_dict()).fillna(self.NOT_FOUND).astype(str)
        Cadence['Child Classes'] = cc.map(armt_lookup1['child_class'].to_dict()).fillna(self.NOT_FOUND).astype(str)
        Cadence['Parent Classes'] = cc.map(armt_lookup1['parent_class'].to_dict()).fillna(self.NOT_FOUND).astype(str)
        Cadence['Source'] = cc.map(armt_lookup1['Source'].to_dict()).fillna(self.NOT_FOUND).astype(str)
        Cadence['Destination'] = cc.map(armt_lookup1['Destination'].to_dict()).fillna(self.NOT_FOUND).astype(str)
        Cadence['ARC'] = cc.map(armt_lookup1['ARC'].to_dict()).fillna(self.NOT_FOUND).astype(str)
        Cadence['risk score'] = cc.map(armt_lookup1['parent_score'].to_dict()).fillna(1)

        # Secondary lookups
        Cadence['program2'] = cc.map(armt_lookup2['program'].to_dict()).fillna(self.NOT_FOUND).astype(str)
        Cadence['Policies2'] = cc.map(armt_lookup2['policy_name'].to_dict()).fillna(self.NOT_FOUND).astype(str)
        Cadence['Child Classes2'] = cc.map(armt_lookup2['child_class'].to_dict()).fillna(self.NOT_FOUND).astype(str)
        Cadence['Parent Classes2'] = cc.map(armt_lookup2['parent_class'].to_dict()).fillna(self.NOT_FOUND).astype(str)
        Cadence['Source2'] = cc.map(armt_lookup2['Source'].to_dict()).fillna(self.NOT_FOUND).astype(str)
        Cadence['Destination2'] = cc.map(armt_lookup2['Destination'].to_dict()).fillna(self.NOT_FOUND).astype(str)
        Cadence['ARC2'] = cc.map(armt_lookup2['ARC'].to_dict()).fillna(self.NOT_FOUND).astype(str)
        Cadence['risk score2'] = cc.map(armt_lookup2['parent_score'].to_dict()).fillna(1)

        # Outflow lookups
        resolved_dict1 = {k: self.date_to_string(v) for k, v in outflow_lookup1['Resolved date'].to_dict().items()}
        resolved_dict2 = {k: self.date_to_string(v) for k, v in outflow_lookup2['Resolved date'].to_dict().items()}

        Cadence['Resolved Date'] = cc.map(resolved_dict1).fillna(self.NOT_FOUND).astype(str)
        Cadence['Root Cause'] = cc.map(outflow_lookup1['root_cause'].to_dict()).fillna(self.NOT_FOUND).astype(str)
        Cadence['Resolved Date2'] = cc.map(resolved_dict2).fillna(self.NOT_FOUND).astype(str)
        Cadence['Root Cause2'] = cc.map(outflow_lookup2['root_cause'].to_dict()).fillna(self.NOT_FOUND).astype(str)

        # NC counts
        Cadence['NC Count'] = cc.map(nc_lookup1).apply(lambda x: self.NOT_FOUND if pd.isna(x) else str(x))
        Cadence['NC Count2'] = cc.map(nc_lookup2).apply(lambda x: self.NOT_FOUND if pd.isna(x) else str(x))

        # Risk scores
        Cadence['risk score'] = self.safe_to_numeric(Cadence['risk score']).fillna(1)
        Cadence['risk score2'] = self.safe_to_numeric(Cadence['risk score2']).fillna(1)

        # Cadence scores
        self.log('   Calculating cadence scores...')
        Cadence['Cadence Score'] = Cadence['risk score'].map(self.RISK_TO_CADENCE).fillna(30).astype(int)
        Cadence['Cadence Score2'] = Cadence['risk score2'].map(self.RISK_TO_CADENCE).fillna(30).astype(int)

        # JSR status
        Cadence['JSR'] = Cadence['Policies'].str.contains('_JSR|-JSR', na=False, regex=True).map(
            {True: 'Yes', False: 'No'})

        self.log(f'✅ IAS Cadence created: {len(Cadence):,} records')
        return Cadence

    def apply_master_lookups(self, Cadence, master):
        """Apply master lookups for IAS"""
        self.log('🔗 Applying IAS master lookups...')

        master['Combined Classes'] = (
                master['Parent Classes'].astype(str) + "," +
                master['Child Classes'].astype(str) + ',' +
                master['Source'].astype(str)
        )
        master['Combined Classes2'] = (
                master['Parent Classes'].astype(str) + "," +
                master['Child Classes'].astype(str) + ',' +
                master['ARC'].astype(str)
        )

        master_lookup1 = master.drop_duplicates(subset=['Combined Classes']).set_index('Combined Classes')
        master_lookup2 = master.drop_duplicates(subset=['Combined Classes2']).set_index('Combined Classes2')

        cc = Cadence['Combined Classes']

        # Previous Cadence
        Cadence['Previous Cadence'] = cc.map(master_lookup1['Cadence Score'].to_dict()).apply(
            lambda x: self.NOT_FOUND if pd.isna(x) else str(int(x)) if isinstance(x, (int, float)) and not pd.isna(
                x) else str(x)
        )
        Cadence['Previous Cadence2'] = cc.map(master_lookup2['Cadence Score'].to_dict()).apply(
            lambda x: self.NOT_FOUND if pd.isna(x) else str(int(x)) if isinstance(x, (int, float)) and not pd.isna(
                x) else str(x)
        )

        # Previous Due Date
        due_date_dict1 = {k: self.date_to_string(v) for k, v in master_lookup1['Due Date'].to_dict().items()}
        due_date_dict2 = {k: self.date_to_string(v) for k, v in master_lookup2['Due Date'].to_dict().items()}

        Cadence['Previous Due Date'] = cc.map(due_date_dict1).fillna(self.NOT_FOUND).astype(str)
        Cadence['Previous Due Date2'] = cc.map(due_date_dict2).fillna(self.NOT_FOUND).astype(str)

        # Previous NC
        if 'NC Count' in master.columns:
            Cadence['Previous NC'] = cc.map(master_lookup1['NC Count'].to_dict()).apply(
                lambda x: self.NOT_FOUND if pd.isna(x) else str(x)
            )
        else:
            Cadence['Previous NC'] = self.NOT_FOUND

        self.log('✅ IAS Master lookups applied')
        return Cadence, master

    def merge_columns(self, Cadence):
        """Merge primary and secondary columns"""
        self.log('🔀 Merging columns...')

        merge_pairs = [
            ('program', 'program2'),
            ('Policies', 'Policies2'),
            ('Child Classes', 'Child Classes2'),
            ('Parent Classes', 'Parent Classes2'),
            ('Resolved Date', 'Resolved Date2'),
            ('Root Cause', 'Root Cause2'),
            ('Previous Cadence', 'Previous Cadence2'),
            ('Previous Due Date', 'Previous Due Date2'),
            ('ARC', 'ARC2'),
            ('Source', 'Source2'),
            ('NC Count', 'NC Count2'),
            ('Destination', 'Destination2'),
        ]

        for primary, secondary in merge_pairs:
            if primary in Cadence.columns and secondary in Cadence.columns:
                mask = Cadence[primary].apply(lambda x: self.is_not_found(x))
                Cadence.loc[mask, primary] = Cadence.loc[mask, secondary]

        self.log('✅ Columns merged')
        return Cadence

    def update_cadence_score(self, Cadence):
        """Update cadence scores based on business rules"""
        self.log('📈 Updating IAS cadence scores...')

        nc_numeric = self.safe_to_numeric(Cadence['NC Count'])
        prev_nc_numeric = self.safe_to_numeric(Cadence['Previous NC'])
        prev_cad_numeric = self.safe_to_numeric(Cadence['Previous Cadence'])
        risk = Cadence['risk score'].astype(float)

        new_score = Cadence['Cadence Score'].astype(float).copy()

        has_prev_cad = prev_cad_numeric.notna()
        has_nc = nc_numeric.notna()
        has_prev_nc = prev_nc_numeric.notna()

        # Business rules (same as BLR)
        mask_30 = has_prev_cad & (prev_cad_numeric == 30)
        new_score = np.where(mask_30 & has_nc & (nc_numeric >= 10), 30, new_score)
        new_score = np.where(mask_30 & has_nc & (nc_numeric < 10) & has_prev_nc & (prev_nc_numeric >= 10), 30,
                             new_score)
        new_score = np.where(mask_30 & (~has_nc) & has_prev_nc & (prev_nc_numeric >= 10), 30, new_score)
        new_score = np.where(mask_30 & has_nc & (nc_numeric < 10) & has_prev_nc & (prev_nc_numeric < 10), 60, new_score)
        new_score = np.where(mask_30 & has_nc & (nc_numeric < 10) & (~has_prev_nc), 60, new_score)

        mask_60 = has_prev_cad & (prev_cad_numeric == 60)
        new_score = np.where(mask_60 & has_nc & (nc_numeric >= 10), 30, new_score)
        new_score = np.where(mask_60 & has_nc & (nc_numeric < 10) & has_prev_nc & (prev_nc_numeric >= 10), 60,
                             new_score)
        new_score = np.where(mask_60 & (~has_nc) & has_prev_nc & (prev_nc_numeric >= 10), 60, new_score)
        new_score = np.where(mask_60 & has_nc & (nc_numeric < 10) & has_prev_nc & (prev_nc_numeric < 10), 90, new_score)
        new_score = np.where(mask_60 & has_nc & (nc_numeric < 10) & (~has_prev_nc), 90, new_score)

        mask_90 = has_prev_cad & (prev_cad_numeric == 90)
        new_score = np.where(mask_90 & has_nc & (nc_numeric >= 10), 60, new_score)
        new_score = np.where(mask_90 & has_nc & (nc_numeric < 10) & has_prev_nc & (prev_nc_numeric >= 10), 90,
                             new_score)
        new_score = np.where(mask_90 & (~has_nc) & has_prev_nc & (prev_nc_numeric >= 10), 90, new_score)
        new_score = np.where(
            mask_90 & has_nc & (nc_numeric < 10) & has_prev_nc & (prev_nc_numeric < 10) & (risk.isin([1, 2, 3])), 90,
            new_score)
        new_score = np.where(
            mask_90 & has_nc & (nc_numeric < 10) & has_prev_nc & (prev_nc_numeric < 10) & (risk.isin([4, 5])), 180,
            new_score)
        new_score = np.where(mask_90 & has_nc & (nc_numeric < 10) & (~has_prev_nc) & (risk.isin([1, 2, 3])), 90,
                             new_score)
        new_score = np.where(mask_90 & has_nc & (nc_numeric < 10) & (~has_prev_nc) & (risk.isin([4, 5])), 180,
                             new_score)

        mask_180 = has_prev_cad & (prev_cad_numeric == 180)
        new_score = np.where(mask_180 & has_nc & (nc_numeric >= 15), 90, new_score)
        new_score = np.where(mask_180 & has_nc & (nc_numeric < 15) & has_prev_nc & (prev_nc_numeric >= 15), 180,
                             new_score)
        new_score = np.where(mask_180 & (~has_nc) & has_prev_nc & (prev_nc_numeric >= 15), 180, new_score)
        new_score = np.where(mask_180 & has_nc & (nc_numeric < 15) & has_prev_nc & (prev_nc_numeric < 15) & (risk == 4),
                             180, new_score)
        new_score = np.where(mask_180 & has_nc & (nc_numeric < 15) & has_prev_nc & (prev_nc_numeric < 15) & (risk != 4),
                             365, new_score)
        new_score = np.where(mask_180 & has_nc & (nc_numeric < 15) & (~has_prev_nc) & (risk == 4), 180, new_score)
        new_score = np.where(mask_180 & has_nc & (nc_numeric < 15) & (~has_prev_nc) & (risk != 4), 365, new_score)

        mask_365 = has_prev_cad & (prev_cad_numeric == 365)
        new_score = np.where(mask_365 & has_nc & (nc_numeric >= 15), 180, new_score)
        new_score = np.where(mask_365 & has_nc & (nc_numeric < 15) & has_prev_nc & (prev_nc_numeric >= 15), 365,
                             new_score)
        new_score = np.where(mask_365 & (~has_nc) & has_prev_nc & (prev_nc_numeric >= 15), 365, new_score)
        new_score = np.where(mask_365 & has_nc & (nc_numeric < 15) & has_prev_nc & (prev_nc_numeric < 15), 365,
                             new_score)
        new_score = np.where(mask_365 & has_nc & (nc_numeric < 15) & (~has_prev_nc), 365, new_score)

        Cadence['Cadence Score'] = pd.Series(new_score).astype(int)

        self.log('✅ IAS Cadence scores updated')
        return Cadence

    def calculate_due_dates(self, Cadence):
        """Calculate due dates for IAS"""
        self.log('📅 Calculating IAS due dates...')

        Cadence['Due Date'] = self.NOT_FOUND

        nc_numeric = self.safe_to_numeric(Cadence['NC Count'])
        cadence_score = Cadence['Cadence Score'].astype(float)

        self.log('   Processing due dates...')

        due_dates = []
        for idx in range(len(Cadence)):
            nc = nc_numeric.iloc[idx]
            cad = cadence_score.iloc[idx]
            resolved = Cadence['Resolved Date'].iloc[idx]

            if pd.isna(nc) or self.is_not_found(resolved):
                due_dates.append(self.NOT_FOUND)
                continue

            days = None
            if nc < 10:
                if cad == 30:
                    days = 30
                elif cad == 60:
                    days = 60
                elif cad == 90:
                    days = 90
            if nc < 15:
                if cad == 180:
                    days = 180
                elif cad == 365:
                    days = 365
            if nc >= 10:
                if cad == 30:
                    days = 30
                elif cad == 60:
                    days = 30
                elif cad == 90:
                    days = 60
            if nc >= 15:
                if cad == 180:
                    days = 90
                elif cad == 365:
                    days = 180

            if days is not None:
                due_dates.append(self.add_days_to_date(resolved, days))
            else:
                due_dates.append(self.NOT_FOUND)

        Cadence['Due Date'] = due_dates

        # Fallback to previous values
        self.log('   Handling fallback to previous values...')
        for idx in range(len(Cadence)):
            if self.is_not_found(Cadence['NC Count'].iloc[idx]):
                prev_cad = Cadence['Previous Cadence'].iloc[idx]
                if not self.is_not_found(prev_cad):
                    try:
                        Cadence.at[idx, 'Cadence Score'] = int(float(prev_cad))
                    except (ValueError, TypeError):
                        pass
                prev_due = Cadence['Previous Due Date'].iloc[idx]
                if not self.is_not_found(prev_due):
                    Cadence.at[idx, 'Due Date'] = str(prev_due)

        self.log('✅ IAS Due dates calculated')
        return Cadence

    def finalize_cadence(self, Cadence):
        """Finalize IAS cadence calculations"""
        self.log('✨ Finalizing IAS cadence...')

        cols_to_drop = [
            'program2', 'Policies2', 'Child Classes2', 'Parent Classes2',
            'Resolved Date2', 'Root Cause2', 'Cadence Score2', 'Previous Cadence2',
            'Previous Due Date2', 'NC Count2', 'risk score2', 'Source2',
            'Destination2', 'ARC2'
        ]
        Cadence = Cadence.drop(cols_to_drop, axis=1, errors='ignore')

        Cadence.loc[Cadence['risk score'] == 0, 'risk score'] = 1
        Cadence['Cadence Score'] = pd.to_numeric(Cadence['Cadence Score'], errors='coerce').fillna(30).astype(int)
        Cadence['Due Date'] = Cadence['Due Date'].apply(lambda x: self.date_to_string(x))

        self.log('✅ IAS Cadence finalized')
        return Cadence

    def process(self, files_dict, output_month="December"):
        """Main processing function for IAS node"""
        self.clear_logs()
        self.log(f'🚀 Starting IAS Cadence processing for {output_month}')
        self.log('=' * 50)

        try:
            self.log("📖 Reading input files...")
            armt = pd.read_excel(files_dict['armt_file'], sheet_name='Sheet1')
            self.log(f"   ARMT: {len(armt):,} rows")

            master = pd.read_excel(files_dict['master_file'], sheet_name='Sheet1')
            self.log(f"   Master: {len(master):,} rows")

            outflow = pd.read_excel(files_dict['outflow_file'], sheet_name='Sheet1')
            self.log(f"   Outflow: {len(outflow):,} rows")

            self.log('=' * 50)
            armt = self.process_armt(armt)

            self.log('=' * 50)
            outflow, outflow1, outflow2 = self.process_outflow(outflow)

            self.log('=' * 50)
            nodes = self.create_nodes(armt)

            if len(nodes) == 0:
                self.log("⚠️ No IAS nodes created - check your ARMT data")
                return {
                    'success': False,
                    'error': 'No valid IAS nodes found in ARMT data',
                    'logs': self.get_logs()
                }

            self.log('=' * 50)
            Cadence = self.create_cadence(nodes, armt, outflow, outflow1, outflow2)

            self.log('=' * 50)
            Cadence, master = self.apply_master_lookups(Cadence, master)

            self.log('=' * 50)
            Cadence = self.merge_columns(Cadence)

            self.log('=' * 50)
            Cadence = self.update_cadence_score(Cadence)

            self.log('=' * 50)
            Cadence = self.calculate_due_dates(Cadence)

            self.log('=' * 50)
            Cadence = self.finalize_cadence(Cadence)

            Cadence_filtered = Cadence[Cadence['Cadence Score'] > 0].copy()

            self.log('=' * 50)
            self.log(f'🎉 IAS Cadence for {output_month} is ready!')
            self.log(f'📊 Total records: {len(Cadence):,}')
            self.log(f'✅ Valid records: {len(Cadence_filtered):,}')

            return {
                'cadence': Cadence,
                'cadence_filtered': Cadence_filtered,
                'master': master,
                'armt': armt,
                'success': True,
                'logs': self.get_logs(),
                'node': 'IAS'
            }

        except Exception as e:
            import traceback
            self.log(f"❌ Error: {str(e)}")
            self.log(f"📋 Details: {traceback.format_exc()}")
            return {
                'success': False,
                'error': str(e),
                'logs': self.get_logs()
            }