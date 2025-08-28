#!/usr/bin/env python3
from flask import Flask, request, jsonify
import requests
import json
import logging
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
import threading
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

AURORA_CONFIG = {
    'tenant_id': '034b1c47-310a-460f-9d5d-b625dd354f12',
    'api_key': os.environ.get('AURORA_API_KEY', 'rk_prod_eff6929551ffeb939d848ef0'),
    'base_url': 'https://api.aurorasolar.com'
}

QUICKBASE_CONFIG = {
    'realm': 'kin.quickbase.com',
    'user_token': os.environ.get('QUICKBASE_TOKEN', 'b6um6p_p3bs_0_bmrupwzbc82cdnb44a7pirtbxif'),
    'app_id': 'bvdg9ck3u',
    'table_id': os.environ.get('QUICKBASE_TABLE_ID', 'bvdzbdbe2')
}

# VALID FIELDS - Based on your actual Quickbase table schema
VALID_FIELDS = {
    # From your Quickbase table (confirmed existing fields)
    1,   # Date Created
    2,   # Date Modified  
    3,   # Record ID
    4,   # Record Owner
    5,   # Last Modified By
    6,   # Design ID
    7,   # Project ID
    8,   # Event Type
    9,   # Received At
    10,  # Summary JSON
    11,  # Roof JSON
    12,  # Pricing JSON
    13,  # Cost Payload JSON
    14,  # Version
    15,  # Hash / Idempotency Key
    16,  # Processing Status
    17,  # Error Message
    18,  # Stage
    19,  # Arrays Count
    20,  # Modules Total
    21,  # System DC kW
    22,  # System AC kW
    23,  # Portrait Modules
    24,  # Landscape Modules
    25,  # Inverters Qty
    26,  # Optimizers Qty
    27,  # Microinverters Qty
    28,  # MLPE Type
    29,  # Arrays Summary
    30,  # Modules Qty
    32,  # Racking Attachments Qty
    33,  # Rails Length Ft
    34,  # Wire Length Ft
    36,  # Combiners Qty
    37,  # Disconnects Qty
    38,  # Breakers Qty
    39,  # Grounding Lugs Qty
    40,  # Other BOM JSON
    41,  # Base Price
    42,  # Adders JSON
    43,  # Incentives JSON
    44,  # BOM Lines Count
    45,  # Simulation Captured?
    46,  # Module Model
    47,  # Module Wattage
    48,  # Inverter Model
    49,  # MLPE Model
    50,  # MLPE Qty
    51,  # Annual Production
    52,  # Annual Offset
    53,  # Monthly Production
    54,  # String Inverter ID
    56,  # String Inverter Rated Power
    57,  # Microinverter ID
    58,  # Microinverter Name
    59,  # Microinverter Count
    60,  # Microinverter Rated Power
    61,  # Module ID
    62,  # Module STC
    63,  # DC Optimizer ID
    64,  # DC Optimizer Name
    65,  # Solar Access Annual
    66,  # Solar Access Monthly
    67,  # TSRF Value
    68,  # Module Manufacture - BOM
    69,  # String Inverter Manufacturer - BOM
    70,  # DC Optimizer Manufacturer - BOM
    71,  # DC Optimizer Quantity
    72,  # Microinverter Manufacturer - BOM
    73,  # BOM JSON
    74,  # Shading JSON
    75,  # Roof Metrics
    80,  # Module SKU - BOM
    81,  # Inverter SKU - BOM
    82,  # DC Optimizer SKU - BOM
    83,  # Microinverter SKU - BOM
    84,  # Racking Component SKU - BOM
    85,  # Battery SKU - BOM
    86,  # Combiner Box SKU - BOM
    87,  # Disconnects SKU - BOM
    91   # Customer Name
    
    # REMOVED - These fields don't exist in your table:
    # 76, 77, 78, 79, 88, 89
}

class AuroraSolarClient:
    def __init__(self):
        self.tenant_id = AURORA_CONFIG['tenant_id']
        self.api_key = AURORA_CONFIG['api_key']
        self.base_url = AURORA_CONFIG['base_url']
        self.headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        }
    
    def get_design_summary(self, design_id: str) -> Optional[Dict]:
        url = f"{self.base_url}/tenants/{self.tenant_id}/designs/{design_id}/summary"
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching design {design_id}: {str(e)}")
            return None
    
    def get_project(self, project_id: str) -> Optional[Dict]:
        """Fetch project details including customer information"""
        url = f"{self.base_url}/tenants/{self.tenant_id}/projects/{project_id}"
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching project {project_id}: {str(e)}")
            return None

class QuickbaseClient:
    def __init__(self):
        self.realm = QUICKBASE_CONFIG['realm']
        self.user_token = QUICKBASE_CONFIG['user_token']
        self.app_id = QUICKBASE_CONFIG['app_id']
        self.table_id = QUICKBASE_CONFIG['table_id']
        self.headers = {
            'QB-Realm-Hostname': self.realm,
            'Authorization': f'QB-USER-TOKEN {self.user_token}',
            'Content-Type': 'application/json'
        }
    
    def validate_field_data(self, data: Dict[int, Any]) -> Dict[int, Any]:
        """Validate and clean field data before sending to Quickbase"""
        cleaned_data = {}
        
        for field_id, field_data in data.items():
            # Skip fields that don't exist in the table
            if field_id not in VALID_FIELDS:
                logger.warning(f"Skipping unknown field {field_id}")
                continue
            
            value = field_data.get('value')
            
            # Data type validation and cleaning
            if value is None:
                continue  # Skip null values
                
            # Handle string fields - ensure they're not too long
            if isinstance(value, str):
                if len(value) > 10000:  # Quickbase text field limit
                    value = value[:10000]
                    logger.warning(f"Truncated field {field_id} to 10000 characters")
                
                # Clean up JSON strings
                if value.startswith('{') or value.startswith('['):
                    try:
                        # Validate JSON
                        json.loads(value)
                    except json.JSONDecodeError:
                        logger.warning(f"Invalid JSON in field {field_id}, setting to empty")
                        value = '{}' if value.startswith('{') else '[]'
            
            # Handle numeric fields - ensure they're valid numbers
            elif isinstance(value, (int, float)):
                if value != value:  # Check for NaN
                    value = 0
                    logger.warning(f"NaN value in field {field_id}, setting to 0")
            
            cleaned_data[field_id] = {'value': value}
        
        return cleaned_data
    
    def get_table_fields(self) -> Optional[Dict]:
        """Get table schema to validate field existence"""
        url = f"https://api.quickbase.com/v1/fields?tableId={self.table_id}"
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching table fields: {str(e)}")
            return None
    
    def upsert_record(self, data: Dict[int, Any]) -> bool:
        if not self.table_id:
            logger.error("Table ID not set")
            return False
        
        # Validate and clean the data
        cleaned_data = self.validate_field_data(data)
        
        if not cleaned_data:
            logger.error("No valid fields to send to Quickbase")
            return False
        
        url = f"https://api.quickbase.com/v1/records"
        
        logger.info(f"Attempting to upsert to table {self.table_id}")
        logger.info(f"Sending {len(cleaned_data)} fields to Quickbase")
        
        # Log first few fields for debugging
        sample_data = {k: v for k, v in list(cleaned_data.items())[:3]}
        logger.info(f"Sample fields: {sample_data}")
        
        body = {
            'to': self.table_id,
            'data': [cleaned_data],
            'mergeFieldId': 6,  # Design ID field
            'fieldsToReturn': [3, 6]  # Return record ID and design ID
        }
        
        try:
            response = requests.post(url, json=body, headers=self.headers)
            logger.info(f"Response status code: {response.status_code}")
            
            if response.status_code == 207:
                # Multi-status response - some succeeded, some failed
                result = response.json()
                logger.warning("Received 207 Multi-Status response")
                
                # Check for line errors
                if 'lineErrors' in result:
                    logger.error("Line errors in Quickbase response:")
                    for error in result['lineErrors']:
                        logger.error(f"  {error}")
                
                # Check if we got any successful data back
                if 'data' in result and len(result['data']) > 0:
                    record_data = result['data'][0]
                    record_id = record_data.get('3', {}).get('value', 'NO_RECORD_ID')
                    design_id = record_data.get('6', {}).get('value', 'NO_DESIGN_ID')
                    logger.info(f"✓ Record created/updated - ID: {record_id}, Design: {design_id}")
                    return True
                else:
                    logger.error("No data returned despite 207 status - record may not have been created")
                    return False
            
            elif response.status_code == 200:
                # Success
                result = response.json()
                if 'data' in result and len(result['data']) > 0:
                    record_data = result['data'][0]
                    record_id = record_data.get('3', {}).get('value', 'NO_RECORD_ID')
                    design_id = record_data.get('6', {}).get('value', 'NO_DESIGN_ID')
                    logger.info(f"✓ Successfully created/updated record - ID: {record_id}, Design: {design_id}")
                    return True
                else:
                    logger.error("No data returned from Quickbase")
                    return False
            
            else:
                # Error status
                response.raise_for_status()
                
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP Error: {e}")
            error_response = e.response.text if e.response else 'No response'
            logger.error(f"Error response: {error_response}")
            
            # Try to parse error details
            try:
                error_data = json.loads(error_response)
                if 'errors' in error_data:
                    for error in error_data['errors']:
                        logger.error(f"Quickbase error: {error}")
            except:
                pass
            
            return False
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            return False

def safe_numeric_value(value, default=0):
    """Safely convert value to number, return default if invalid"""
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return value if not (value != value) else default  # Check for NaN
    if isinstance(value, str):
        try:
            return float(value) if '.' in value else int(value)
        except (ValueError, TypeError):
            return default
    return default

def safe_string_value(value, default='N/A', max_length=1000):
    """Safely convert value to string, return default if invalid"""
    if value is None:
        return default
    if isinstance(value, str):
        return value[:max_length] if len(value) > max_length else value
    try:
        return str(value)[:max_length]
    except:
        return default

def transform_data(design_data: Dict) -> Dict[int, Any]:
    """Transform Aurora design data to Quickbase format with robust error handling"""
    qb_record = {}
    design = design_data.get('design', {})
    project = design_data.get('project', {})
    
    try:
        # Core design fields with safe conversion
        qb_record[6] = {'value': safe_string_value(design.get('design_id', ''))}
        qb_record[7] = {'value': safe_string_value(design.get('project_id', ''))}
        
        # System sizes - convert W to kW safely
        system_dc = safe_numeric_value(design.get('system_size_stc', 0)) / 1000
        system_ac = safe_numeric_value(design.get('system_size_ac', 0)) / 1000
        qb_record[21] = {'value': round(system_dc, 2)}
        qb_record[22] = {'value': round(system_ac, 2)}
        
        # Status fields
        qb_record[16] = {'value': 'Installed'}
        qb_record[17] = {'value': 'Completed'}
        qb_record[18] = {'value': 'Installed'}
        
        # Customer information with safe handling
        if project:
            customer = project.get('customer', {})
            if customer:
                first_name = safe_string_value(customer.get('first_name', ''), '')
                last_name = safe_string_value(customer.get('last_name', ''), '')
                customer_name = f"{first_name} {last_name}".strip()
                if not customer_name:
                    customer_name = safe_string_value(customer.get('name', ''), 'N/A')
                qb_record[91] = {'value': customer_name}
            else:
                qb_record[91] = {'value': 'N/A'}
        else:
            qb_record[91] = {'value': 'N/A'}
        
        # Energy production with safe handling
        energy = design.get('energy_production', {})
        if energy:
            qb_record[51] = {'value': safe_numeric_value(energy.get('annual', 0))}
            
            # Handle annual offset
            annual_offset = energy.get('annual_offset', 0)
            if isinstance(annual_offset, str):
                annual_offset = annual_offset.replace('%', '')
            qb_record[52] = {'value': safe_string_value(annual_offset, '0')}
            
            qb_record[45] = {'value': bool(energy.get('up_to_date', False))}
            
            # Monthly data as JSON
            if energy.get('monthly'):
                monthly_json = json.dumps(energy['monthly'])[:5000]
                qb_record[53] = {'value': monthly_json}
        
        # Process arrays with comprehensive error handling
        arrays = design.get('arrays', [])
        total_modules = 0
        portrait_count = 0
        landscape_count = 0
        total_dc_optimizers = 0
        total_microinverters = 0
        
        if arrays:
            qb_record[19] = {'value': len(arrays)}
            
            # Module tracking
            first_module_name = ''
            first_module_id = ''
            first_module_rating = 0
            
            # MLPE tracking
            first_dc_optimizer_id = ''
            first_dc_optimizer_name = ''
            first_microinverter_id = ''
            first_microinverter_name = ''
            
            for array in arrays:
                try:
                    module = array.get('module', {})
                    if module:
                        count = safe_numeric_value(module.get('count', 0))
                        total_modules += count
                        
                        # Orientation detection
                        orientation = safe_string_value(module.get('orientation', ''), '').lower()
                        if 'landscape' in orientation:
                            landscape_count += count
                        else:
                            portrait_count += count  # Default to portrait
                        
                        # Store first module info
                        if not first_module_name:
                            first_module_name = safe_string_value(module.get('name', ''))
                            first_module_id = safe_string_value(module.get('id', ''))
                            first_module_rating = safe_numeric_value(module.get('rating_stc', 0))
                    
                    # Process microinverters
                    microinverter = array.get('microinverter', {})
                    if microinverter:
                        micro_count = safe_numeric_value(microinverter.get('count', 0))
                        total_microinverters += micro_count
                        
                        if not first_microinverter_id:
                            first_microinverter_id = safe_string_value(microinverter.get('id', ''))
                            first_microinverter_name = safe_string_value(microinverter.get('name', ''))
                            qb_record[57] = {'value': first_microinverter_id}
                            qb_record[58] = {'value': first_microinverter_name}
                            qb_record[60] = {'value': safe_numeric_value(microinverter.get('rated_power', 0))}
                    
                    # Process DC optimizers
                    dc_optimizer = array.get('dc_optimizer', {})
                    if dc_optimizer:
                        dc_count = safe_numeric_value(dc_optimizer.get('count', 0))
                        total_dc_optimizers += dc_count
                        
                        if not first_dc_optimizer_id:
                            first_dc_optimizer_id = safe_string_value(dc_optimizer.get('id', ''))
                            first_dc_optimizer_name = safe_string_value(dc_optimizer.get('name', ''))
                            qb_record[63] = {'value': first_dc_optimizer_id}
                            qb_record[64] = {'value': first_dc_optimizer_name}
                
                except Exception as e:
                    logger.warning(f"Error processing array: {e}")
                    continue
            
            # Set module fields
            qb_record[46] = {'value': first_module_name}
            qb_record[61] = {'value': first_module_id}
            qb_record[62] = {'value': first_module_rating}
            qb_record[47] = {'value': first_module_rating}
            
            # Set panel counts
            qb_record[20] = {'value': total_modules}      # Modules Total
            qb_record[30] = {'value': total_modules}      # Modules Qty (duplicate)
            qb_record[23] = {'value': portrait_count}     # Portrait Modules
            qb_record[24] = {'value': landscape_count}    # Landscape Modules
            
            # Set totals for DC optimizers and microinverters
            if total_dc_optimizers > 0:
                qb_record[26] = {'value': total_dc_optimizers}  # Optimizers Qty
                qb_record[71] = {'value': total_dc_optimizers}  # DC Optimizer Quantity
                logger.info(f"Total DC Optimizers across all arrays: {total_dc_optimizers}")
            
            if total_microinverters > 0:
                qb_record[27] = {'value': total_microinverters}  # Microinverters Qty
                qb_record[59] = {'value': total_microinverters}  # Microinverter Count
                logger.info(f"Total Microinverters across all arrays: {total_microinverters}")
            
            # Log counts for debugging
            logger.info(f"Panel counts - Total: {total_modules}, Portrait: {portrait_count}, Landscape: {landscape_count}")
            
            # Calculate average solar access and TSRF
            solar_access_sum = 0
            tsrf_sum = 0
            arrays_with_shading = 0
            
            for array in arrays:
                try:
                    shading = array.get('shading', {})
                    if shading:
                        solar_access = shading.get('solar_access', {}).get('annual')
                        tsrf = shading.get('total_solar_resource_fraction', {}).get('annual')
                        
                        if solar_access is not None:
                            solar_access_sum += safe_numeric_value(solar_access)
                            arrays_with_shading += 1
                        
                        if tsrf is not None:
                            tsrf_sum += safe_numeric_value(tsrf)
                except Exception as e:
                    logger.warning(f"Error processing shading data: {e}")
                    continue
            
            if arrays_with_shading > 0:
                qb_record[65] = {'value': round(solar_access_sum / arrays_with_shading, 2)}
                qb_record[67] = {'value': round(tsrf_sum / arrays_with_shading, 2)}
            
            # Store raw arrays data (truncated for field 29)
            try:
                arrays_json = json.dumps(arrays)[:1000]
                qb_record[29] = {'value': arrays_json}
            except Exception as e:
                logger.warning(f"Error serializing arrays data: {e}")
                qb_record[29] = {'value': '[]'}
        
        # Process string inverters
        string_inverters = design.get('string_inverters', [])
        if string_inverters:
            qb_record[25] = {'value': len(string_inverters)}
            inv = string_inverters[0]
            qb_record[48] = {'value': safe_string_value(inv.get('name', ''))}
            qb_record[54] = {'value': safe_string_value(inv.get('id', ''))}
            qb_record[56] = {'value': safe_numeric_value(inv.get('rated_power', 0))}
            qb_record[69] = {'value': safe_string_value(inv.get('manufacturer', ''), 'N/A')}
        
        # Process Bill of Materials - Comprehensive with error handling
        bom = design.get('bill_of_materials', [])
        if bom:
            qb_record[44] = {'value': len(bom)}  # BOM Lines Count
            
            # Initialize BOM quantities
            racking_qty = 0
            
            for item in bom:
                try:
                    ct = safe_string_value(item.get('component_type', '')).lower()
                    sku = safe_string_value(item.get('sku', ''), 'N/A')
                    mfg = safe_string_value(item.get('manufacturer_name', ''), 'N/A')
                    qty = safe_numeric_value(item.get('quantity', 0))
                    
                    if ct == 'modules':
                        qb_record[80] = {'value': sku}  # Module SKU
                        qb_record[68] = {'value': mfg}  # Module Manufacturer
                    elif ct == 'inverters':
                        qb_record[81] = {'value': sku}  # Inverter SKU
                        qb_record[69] = {'value': mfg}  # String Inverter Manufacturer
                    elif ct == 'microinverters':
                        qb_record[83] = {'value': sku}  # Microinverter SKU
                        qb_record[72] = {'value': mfg}  # Microinverter Manufacturer
                    elif ct == 'dc_optimizers':
                        qb_record[82] = {'value': sku}  # DC Optimizer SKU
                        qb_record[70] = {'value': mfg}  # DC Optimizer Manufacturer
                    elif ct == 'batteries':
                        qb_record[85] = {'value': sku}  # Battery SKU
                    elif ct == 'combiner_boxes':
                        qb_record[86] = {'value': sku}  # Combiner Box SKU
                        qb_record[36] = {'value': qty}  # Combiners Qty
                    elif ct == 'disconnects':
                        qb_record[87] = {'value': sku}  # Disconnects SKU
                        qb_record[37] = {'value': qty}  # Disconnects Qty
                    elif ct in ['racking_components', 'racking']:
                        racking_qty += qty
                        if not qb_record.get(84):  # Only set first racking SKU
                            qb_record[84] = {'value': sku}  # Racking Component SKU
                
                except Exception as e:
                    logger.warning(f"Error processing BOM item: {e}")
                    continue
            
            # Set racking total
            if racking_qty > 0:
                qb_record[32] = {'value': racking_qty}  # Racking Attachments Qty
            
            # Store full BOM as JSON (truncated)
            try:
                bom_json = json.dumps(bom)[:10000]
                qb_record[73] = {'value': bom_json}
            except Exception as e:
                logger.warning(f"Error serializing BOM data: {e}")
                qb_record[73] = {'value': '[]'}
        
        # Store full design data as JSON (truncated for field 10)
        try:
            design_json = json.dumps(design_data)[:10000]
            qb_record[10] = {'value': design_json}
        except Exception as e:
            logger.warning(f"Error serializing design data: {e}")
            qb_record[10] = {'value': '{}'}
        
        # Set MLPE Type field (28 based on schema)
        mlpe_type = 'N/A'
        if total_dc_optimizers > 0:
            mlpe_type = 'DC Optimizer'
        elif total_microinverters > 0:
            mlpe_type = 'Microinverter'
        qb_record[28] = {'value': mlpe_type}  # MLPE Type
        qb_record[49] = {'value': mlpe_type}  # MLPE Model (if different usage)
        
        # Set defaults for fields that weren't populated
        field_defaults = {
            # Numeric fields - default to 0
            19: 0,   # Arrays Count
            20: 0,   # Modules Total
            23: 0,   # Portrait Modules
            24: 0,   # Landscape Modules
            25: 0,   # Inverters Qty
            26: 0,   # Optimizers Qty
            27: 0,   # Microinverters Qty
            30: 0,   # Modules Qty
            32: 0,   # Racking Attachments Qty
            33: 0,   # Rails Length Ft
            34: 0,   # Wire Length Ft
            36: 0,   # Combiners Qty
            37: 0,   # Disconnects Qty
            38: 0,   # Breakers Qty
            39: 0,   # Grounding Lugs Qty
            41: 0,   # Base Price
            44: 0,   # BOM Lines Count
            47: 0,   # Module Wattage
            50: 0,   # MLPE Qty
            51: 0,   # Annual Production
            56: 0,   # String Inverter Rated Power
            59: 0,   # Microinverter Count
            60: 0,   # Microinverter Rated Power
            62: 0,   # Module STC
            65: 0,   # Solar Access Annual
            66: 0,   # Solar Access Monthly
            67: 0,   # TSRF Value
            71: 0,   # DC Optimizer Quantity
            
            # Text fields - default to appropriate values
            8: 'webhook',        # Event Type
            10: '{}',           # Summary JSON
            11: '{}',           # Roof JSON
            12: '{}',           # Pricing JSON
            13: '{}',           # Cost Payload JSON
            14: 1,              # Version (numeric)
            15: 'N/A',          # Hash / Idempotency Key
            17: '',             # Error Message (empty for success)
            29: '[]',           # Arrays Summary
            40: '{}',           # Other BOM JSON
            42: '{}',           # Adders JSON
            43: '{}',           # Incentives JSON
            46: 'N/A',          # Module Model
            48: 'N/A',          # Inverter Model
            49: 'N/A',          # MLPE Model
            52: '0',            # Annual Offset
            53: '[]',           # Monthly Production
            54: 'N/A',          # String Inverter ID
            57: 'N/A',          # Microinverter ID
            58: 'N/A',          # Microinverter Name
            61: 'N/A',          # Module ID
            63: 'N/A',          # DC Optimizer ID
            64: 'N/A',          # DC Optimizer Name
            68: 'N/A',          # Module Manufacturer
            69: 'N/A',          # String Inverter Manufacturer
            70: 'N/A',          # DC Optimizer Manufacturer
            72: 'N/A',          # Microinverter Manufacturer
            73: '[]',           # BOM JSON
            74: '{}',           # Shading JSON
            75: 'N/A',          # Roof Metrics
            80: 'N/A',          # Module SKU
            81: 'N/A',          # Inverter SKU
            82: 'N/A',          # DC Optimizer SKU
            83: 'N/A',          # Microinverter SKU
            84: 'N/A',          # Racking Component SKU
            85: 'N/A',          # Battery SKU
            86: 'N/A',          # Combiner Box SKU
            87: 'N/A',          # Disconnects SKU
            91: 'N/A'           # Customer Name
        }
        
        # Apply defaults for missing fields
        for field_id, default_value in field_defaults.items():
            if field_id not in qb_record:
                qb_record[field_id] = {'value': default_value}
        
        return qb_record
    
    except Exception as e:
        logger.error(f"Error in transform_data: {str(e)}")
        # Return minimal valid record on error
        return {
            6: {'value': safe_string_value(design_data.get('design', {}).get('design_id', 'ERROR'))},
            17: {'value': f"Transform error: {str(e)}"}  # Error Message
        }
