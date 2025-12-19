"""
GDN Node Processor
Handles cadence calculation for GDN node (DE - Germany)
GDN uses child_class as the main identifier (different from BLR/IAS)
"""

import pandas as pd
import numpy as np
from datetime import timedelta
import warnings
from .base_processor import BaseProcessor

warnings.filterwarnings('ignore')


class GDNProcessor(BaseProcessor):
    """Processor for GDN node cadence calculations"""

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
        self.node_name = "GDN"

        # GDN specific configurations
        self.GDN_Core = ['DE', 'ANY', 'Any']
        self.GDN_Cross = ['DE', 'TR', 'UK', 'Any']
        self.GDN_Excluded_Sources = [
            'JP', 'FR', 'IT', 'ES', 'CN', 'US', 'AU', 'SG', 'AE', 'IN',
            'SA', 'CA', 'NL', 'EG', 'MX', 'UK'
        ]

    def get_required_files(self):
        """Return list of required files for GDN processor"""
        return [
            {
                'key': 'armt_file',
                'label': 'ARMT File',
                'description': 'ARMT_AGCL.xlsx',
                'help': 'Upload the ARMT AGCL Excel file'
            },
            {
                'key': 'master_file',
                'label': 'GDN Master Cadence File',
                'description': 'Previous Month GDN Cadence',
                'help': 'Upload the previous month\'s GDN cadence file'
            },
            {
                'key': 'outflow_file',
                'label': 'Outflow File',
                'description': 'Monthly Outflow Data',
                'help': 'Upload the outflow Excel file'
            }
        ]

    def get_node_config(self):
        """Return GDN node configuration"""
        return {
            'name': 'GDN',
            'full_name': 'GDN Node (Germany)',
            'description': 'Process cadence for GDN node - Germany (DE)',
            'color': '#dc3545'
        }

    def process_armt(self, armt):
        """Process ARMT data for GDN - Groups by child_class"""
        self.log('📂 Processing ARMT data for GDN...')

        cols_to_drop = [
            'parent_job_status', 'parent_job_type', 'parent_last_updated_time',
            'child_job_status', 'child_job_type', 'child_last_updated_time'
        ]
        armt = armt.drop(cols_to_drop, axis=1, errors='ignore')

        armt['Source'] = armt['source_country'].astype(str).str.strip()
        armt['Destination'] = armt['include_destination_country'].astype(str).str.strip()

        # Handle No-Child cases - replace with parent_class
        self.log('   Handling No-Child cases...')
        for i in range(len(armt)):
            if armt.at[i, 'child_class'] == 'No-Child':
                armt.at[i, 'child_class'] = armt.at[i, 'parent_class']

        # Process AmazonGlobal - Group by child_class
        self.log('   Processing AmazonGlobal (grouped by child_class)...')
        armt_core = armt[armt['program'] == 'AmazonGlobal'].copy()
        if len(armt_core) > 0:
            armt_core = armt_core.groupby(['child_class']).agg({
                'parent_class': lambda x: ','.join(x.astype(str).unique()),
                'policy_name': lambda x: ','.join(x.astype(str).unique()),
                'program': 'first',
                'Source': 'first',
                'Destination': 'first',
                'parent_score': 'first'
            }).reset_index()

        # Process CrossListing - Group by child_class
        self.log('   Processing CrossListing (grouped by child_class)...')
        armt_cross = armt[armt['program'] == 'CrossListing'].copy()
        if len(armt_cross) > 0:
            armt_cross = armt_cross.groupby(['child_class']).agg({
                'parent_class': lambda x: ','.join(x.astype(str).unique()),
                'policy_name': lambda x: ','.join(x.astype(str).unique()),
                'program': 'first',
                'Source': 'first',
                'Destination': 'first',
                'parent_score': 'first'
            }).reset_index()

        armt = pd.concat([armt_core, armt_cross], ignore_index=True)

        self.log(f'✅ ARMT processed: {len(armt):,} records (grouped by child_class)')
        return armt

    def process_outflow(self, outflow):
        """Process outflow data for GDN"""
        self.log('📂 Processing Outflow data for GDN...')

        initial_count = len(outflow)

        outflow = outflow.dropna(subset=['root_cause', 'root_cause_details'])

        # GDN specific groups to remove
        del_groups = [
            'RP - AG Auditors', 'RP - AG Auditors CN', 'RP - AG Auditors ES',
            'RP - AG Auditors FR', 'RP - AG Auditors IT'
        ]
        if 'assigned_to_group' in outflow.columns:
            outflow = outflow[~outflow['assigned_to_group'].isin(del_groups)]

        del_causes = ['Duplicate', 'Other', 'Negative Class']
        outflow = outflow[~outflow['root_cause'].isin(del_causes)]

        outflow['child_class'] = outflow['short_description'].astype(str).str.split(':').str[0].str.strip()
        outflow['parent_class'] = outflow['root_cause_details'].astype(str).str.split('\\\\').str[0].str.strip()

        # Process dates
        outflow['resolved_date'] = outflow['resolved_date'].astype(str)
        outflow['Resolved date'] = outflow['resolved_date'].str.split(' ').str[0]
        outflow['Resolved date'] = pd.to_datetime(outflow['Resolved date'], format='%Y-%m-%d', errors='coerce')
        outflow['Resolved date'] = outflow['Resolved date'].apply(self.date_to_string)
        outflow = outflow.sort_values(by='Resolved date', ascending=False)

        outflow = outflow.reset_index(drop=True)

        # Process resolution
        for i in range(len(outflow)):
            outflow.at[i, 'resolution'] = str(outflow['resolution'].iloc[i]).split("\\")[0]
            if outflow['child_class'].iloc[i] == outflow['parent_class'].iloc[i]:
                outflow.at[i, 'child_class'] = 'No-Child'

        outflow['source'] = outflow['resolution'].str.split('-').str[0]
        outflow['destination'] = outflow['resolution'].str.split('-').str[1]

        outflow['quantity'] = pd.to_numeric(outflow['quantity'], errors='coerce').fillna(0)
        outflow['vendor_id'] = pd.to_numeric(outflow['vendor_id'], errors='coerce').fillna(0).astype(int)
        outflow['NC'] = outflow['quantity'] + outflow['vendor_id']

        # Group by child_class for GDN
        outflow1 = outflow.groupby('child_class', as_index=False)['NC'].sum()

        self.log(f'✅ Outflow processed: {len(outflow):,} records (from {initial_count:,})')

        return outflow.reset_index(drop=True), outflow1

    def create_nodes(self, armt):
        """Create node mapping for GDN based on child_class"""
        self.log('🗺️ Creating GDN node mappings...')

        nodes = set()

        for i in range(len(armt)):
            source = str(armt['Source'].iloc[i]).split(',')
            destination = str(armt['Destination'].iloc[i]).split(',')
            program = str(armt['program'].iloc[i])
            child_class = armt['child_class'].iloc[i]

            # AmazonGlobal - DE source
            for j in source:
                if j.strip() in self.GDN_Core:
                    nodes.add(child_class)

            # CrossListing - destination in GDN_Cross and source not in excluded
            if program == 'CrossListing':
                for k in destination:
                    for t in source:
                        if k.strip() in self.GDN_Cross and t.strip() not in self.GDN_Excluded_Sources:
                            nodes.add(child_class)

        self.log(f'✅ Created {len(nodes):,} unique GDN nodes (by child_class)')
        return list(nodes)

    def create_cadence(self, nodes, armt, outflow, outflow1):
        """Create cadence dataframe for GDN"""
        self.log('⚙️ Creating GDN Cadence dataframe...')

        # GDN uses 'child_class' as the main column
        Cadence = pd.DataFrame({'child_class': nodes})
        total = len(Cadence)
        self.log(f'   Processing {total:,} records...')

        self.log('   Building lookup tables...')

        armt_lookup = armt.drop_duplicates(subset=['child_class']).set_index('child_class')
        outflow_lookup = outflow.drop_duplicates(subset=['child_class']).set_index('child_class')
        nc_lookup = outflow1.set_index('child_class')['NC'].to_dict()

        self.log('   Applying lookups...')

        cc = Cadence['child_class']

        # Lookups
        Cadence['program'] = cc.map(armt_lookup['program'].to_dict()).fillna(self.NOT_FOUND).astype(str)
        Cadence['Policies'] = cc.map(armt_lookup['policy_name'].to_dict()).fillna(self.NOT_FOUND).astype(str)
        Cadence['Parent Classes'] = cc.map(armt_lookup['parent_class'].to_dict()).fillna(self.NOT_FOUND).astype(str)
        Cadence['Source'] = cc.map(armt_lookup['Source'].to_dict()).fillna(self.NOT_FOUND).astype(str)
        Cadence['Destination'] = cc.map(armt_lookup['Destination'].to_dict()).fillna(self.NOT_FOUND).astype(str)
        Cadence['risk score'] = cc.map(armt_lookup['parent_score'].to_dict()).fillna(1)

        # Outflow lookups
        resolved_dict = {k: self.date_to_string(v) for k, v in outflow_lookup['Resolved date'].to_dict().items()}
        Cadence['Resolved Date'] = cc.map(resolved_dict).fillna(self.NOT_FOUND).astype(str)

        # NC Count
        Cadence['NC Count'] = cc.map(nc_lookup).apply(lambda x: self.NOT_FOUND if pd.isna(x) else str(x))

        # Risk score
        Cadence['risk score'] = self.safe_to_numeric(Cadence['risk score']).fillna(1)

        # Cadence score
        self.log('   Calculating cadence scores...')
        Cadence['Cadence Score'] = Cadence['risk score'].map(self.RISK_TO_CADENCE).fillna(30).astype(int)

        # JSR status
        Cadence['JSR'] = Cadence['Policies'].str.contains('_JSR|-JSR', na=False, regex=True).map(
            {True: 'Yes', False: 'No'})

        self.log(f'✅ GDN Cadence created: {len(Cadence):,} records')
        return Cadence

    def apply_master_lookups(self, Cadence, master):
        """Apply master lookups for GDN"""
        self.log('🔗 Applying GDN master lookups...')

        master_lookup = master.drop_duplicates(subset=['child_class']).set_index('child_class')

        cc = Cadence['child_class']

        # Previous Cadence
        Cadence['Previous Cadence'] = cc.map(master_lookup['Cadence Score'].to_dict()).apply(
            lambda x: self.NOT_FOUND if pd.isna(x) else str(int(x)) if isinstance(x, (int, float)) and not pd.isna(
                x) else str(x)
        )

        # Previous Due Date
        due_date_dict = {k: self.date_to_string(v) for k, v in master_lookup['Due Date'].to_dict().items()}
        Cadence['Previous Due Date'] = cc.map(due_date_dict).fillna(self.NOT_FOUND).astype(str)

        # Previous NC
        if 'NC Count' in master.columns:
            Cadence['Previous NC'] = cc.map(master_lookup['NC Count'].to_dict()).apply(
                lambda x: self.NOT_FOUND if pd.isna(x) else str(x)
            )
        else:
            Cadence['Previous NC'] = self.NOT_FOUND

        self.log('✅ GDN Master lookups applied')
        return Cadence, master

    def update_cadence_score(self, Cadence):
        """Update cadence scores for GDN based on business rules"""
        self.log('📈 Updating GDN cadence scores...')

        nc_numeric = self.safe_to_numeric(Cadence['NC Count'])
        prev_nc_numeric = self.safe_to_numeric(Cadence['Previous NC'])
        prev_cad_numeric = self.safe_to_numeric(Cadence['Previous Cadence'])
        risk = Cadence['risk score'].astype(float)

        new_score = Cadence['Cadence Score'].astype(float).copy()

        has_prev_cad = prev_cad_numeric.notna()
        has_nc = nc_numeric.notna()
        has_prev_nc = prev_nc_numeric.notna()

        # Business rules (same as BLR/IAS)
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

        self.log('✅ GDN Cadence scores updated')
        return Cadence

    def calculate_due_dates(self, Cadence):
        """Calculate due dates for GDN - Same logic as BLR/IAS"""
        self.log('📅 Calculating GDN due dates...')

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

            # NC < 10: standard days
            if nc < 10:
                if cad == 30:
                    days = 30
                elif cad == 60:
                    days = 60
                elif cad == 90:
                    days = 90

            # NC < 15: standard days for 180/365
            if nc < 15:
                if cad == 180:
                    days = 180
                elif cad == 365:
                    days = 365

            # NC >= 10: reduced days (SAME AS BLR/IAS)
            if nc >= 10:
                if cad == 30:
                    days = 30
                elif cad == 60:
                    days = 30  # Reduced
                elif cad == 90:
                    days = 60  # Reduced

            # NC >= 15: reduced days (SAME AS BLR/IAS)
            if nc >= 15:
                if cad == 180:
                    days = 90  # Reduced
                elif cad == 365:
                    days = 180  # Reduced

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

        self.log('✅ GDN Due dates calculated')
        return Cadence

    def finalize_cadence(self, Cadence):
        """Finalize GDN cadence calculations"""
        self.log('✨ Finalizing GDN cadence...')

        Cadence.loc[Cadence['risk score'] == 0, 'risk score'] = 1
        Cadence['Cadence Score'] = pd.to_numeric(Cadence['Cadence Score'], errors='coerce').fillna(30).astype(int)
        Cadence['Due Date'] = Cadence['Due Date'].apply(lambda x: self.date_to_string(x))

        self.log('✅ GDN Cadence finalized')
        return Cadence

    def process(self, files_dict, output_month="December"):
        """Main processing function for GDN node"""
        self.clear_logs()
        self.log(f'🚀 Starting GDN Cadence processing for {output_month}')
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
            outflow, outflow1 = self.process_outflow(outflow)

            self.log('=' * 50)
            nodes = self.create_nodes(armt)

            if len(nodes) == 0:
                self.log("⚠️ No GDN nodes created - check your ARMT data")
                return {
                    'success': False,
                    'error': 'No valid GDN nodes found in ARMT data',
                    'logs': self.get_logs()
                }

            self.log('=' * 50)
            Cadence = self.create_cadence(nodes, armt, outflow, outflow1)

            self.log('=' * 50)
            Cadence, master = self.apply_master_lookups(Cadence, master)

            self.log('=' * 50)
            Cadence = self.update_cadence_score(Cadence)

            self.log('=' * 50)
            Cadence = self.calculate_due_dates(Cadence)

            self.log('=' * 50)
            Cadence = self.finalize_cadence(Cadence)

            Cadence_filtered = Cadence[Cadence['Cadence Score'] > 0].copy()

            self.log('=' * 50)
            self.log(f'🎉 GDN Cadence for {output_month} is ready!')
            self.log(f'📊 Total records: {len(Cadence):,}')
            self.log(f'✅ Valid records: {len(Cadence_filtered):,}')

            return {
                'cadence': Cadence,
                'cadence_filtered': Cadence_filtered,
                'master': master,
                'armt': armt,
                'success': True,
                'logs': self.get_logs(),
                'node': 'GDN'
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