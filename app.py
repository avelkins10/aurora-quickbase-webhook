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
    
    def upsert_record(self, data: Dict[int, Any]) -> bool:
        if not self.table_id:
            logger.error("Table ID not set")
            return False
        
        url = f"https://api.quickbase.com/v1/records"
        
        # Log the first few fields being sent
        logger.info(f"Attempting to upsert to table {self.table_id}")
        sample_data = {k: v for k, v in list(data.items())[:5]}
        logger.info(f"Sample fields being sent: {sample_data}")
        
        body = {
            'to': self.table_id,
            'data': [data],
            'mergeFieldId': 6,
            'fieldsToReturn': [3, 6]
        }
        
        try:
            response = requests.post(url, json=body, headers=self.headers)
            logger.info(f"Response status code: {response.status_code}")
            
            # Log response even before checking status
            response_text = response.text[:500] if response.text else "Empty response"
            logger.info(f"Response text: {response_text}")
            
            response.raise_for_status()
            result = response.json()
            
            # Check if we actually got data back
            if 'data' not in result or len(result['data']) == 0:
                logger.error("No data returned from Quickbase - record may not have been created")
                return False
                
            # Log what was created/updated
            record_data = result['data'][0]
            record_id = record_data.get('3', {}).get('value', 'NO_RECORD_ID')
            design_id = record_data.get('6', {}).get('value', 'NO_DESIGN_ID')
            logger.info(f"Quickbase record ID: {record_id}, Design ID: {design_id}")
            
            return True
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP Error: {e}")
            logger.error(f"Response: {e.response.text if e.response else 'No response'}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            return False

def transform_data(design_data: Dict) -> Dict[int, Any]:
    qb_record = {}
    design = design_data.get('design', {})
    project = design_data.get('project', {})  # Project data if available
    
    # Initialize these at the function level so they're always available
    total_dc_optimizers = 0
    total_microinverters = 0
    
    # Basic design fields
    qb_record[6] = {'value': design.get('design_id', '')}
    qb_record[7] = {'value': design.get('project_id', '')}
    qb_record[21] = {'value': design.get('system_size_stc', 0) / 1000}  # Convert W to kW
    qb_record[22] = {'value': design.get('system_size_ac', 0) / 1000}   # Convert W to kW
    qb_record[16] = {'value': 'Installed'}
    qb_record[18] = {'value': 'Installed'}
    
    # Customer information from project
    if project:
        # Field 91 - Customer Name
        customer = project.get('customer', {})
        if customer:
            customer_name = f"{customer.get('first_name', '')} {customer.get('last_name', '')}".strip()
            if not customer_name:
                customer_name = customer.get('name', '')
            qb_record[91] = {'value': customer_name if customer_name else 'N/A'}
        else:
            qb_record[91] = {'value': 'N/A'}
        
        # Additional customer fields if needed
        # You can add more field mappings here as needed
    
    # Energy production data
    energy = design.get('energy_production', {})
    if energy:
        qb_record[51] = {'value': energy.get('annual', 0)}
        # FIX: annual_offset is already a number, not a string with %
        annual_offset = energy.get('annual_offset', 0)
        if isinstance(annual_offset, str):
            qb_record[52] = {'value': annual_offset.replace('%', '')}
        else:
            # It's already a number, just convert to percentage format
            qb_record[52] = {'value': str(annual_offset) if annual_offset else '0'}
        qb_record[45] = {'value': energy.get('up_to_date', False)}
        if energy.get('monthly'):
            qb_record[53] = {'value': json.dumps(energy['monthly'])[:5000]}
    
    # Process arrays with FIXED orientation detection
    arrays = design.get('arrays', [])
    if arrays:
        qb_record[19] = {'value': len(arrays)}  # Number of arrays
        
        total_modules = 0
        landscape_count = 0
        portrait_count = 0
        
        # Variables to store first module/inverter info
        first_module_name = ''
        first_module_id = ''
        first_module_rating = 0
        
        # These are already initialized at function level now
        # total_dc_optimizers = 0
        # total_microinverters = 0
        first_dc_optimizer_id = ''
        first_dc_optimizer_name = ''
        first_microinverter_id = ''
        first_microinverter_name = ''
        
        for array in arrays:
            module = array.get('module', {})
            if module:
                count = module.get('count', 0)
                total_modules += count
                
                # FIX: Get orientation from the module object, not the array
                orientation = module.get('orientation', '').lower()
                logger.info(f"Array module orientation: {orientation}, count: {count}")
                
                if 'landscape' in orientation:
                    landscape_count += count
                elif 'portrait' in orientation:
                    portrait_count += count
                else:
                    # Default to portrait if orientation is not specified
                    portrait_count += count
                    logger.warning(f"Unknown orientation '{orientation}' for {count} modules, defaulting to portrait")
                
                # Store first module info
                if not first_module_name and module.get('name'):
                    first_module_name = module.get('name', '')
                    first_module_id = module.get('id', '')
                    first_module_rating = module.get('rating_stc', 0)
            
            # Process microinverters
            microinverter = array.get('microinverter', {})
            if microinverter:
                micro_count = microinverter.get('count', 0)
                total_microinverters += micro_count
                
                # Store first microinverter info
                if not first_microinverter_id and microinverter.get('id'):
                    first_microinverter_id = microinverter.get('id', '')
                    first_microinverter_name = microinverter.get('name', '')
                    qb_record[57] = {'value': first_microinverter_id}
                    qb_record[58] = {'value': first_microinverter_name}
                    qb_record[60] = {'value': microinverter.get('rated_power', 0)}
            
            # Process DC optimizers
            dc_optimizer = array.get('dc_optimizer', {})
            if dc_optimizer:
                dc_count = dc_optimizer.get('count', 0)
                total_dc_optimizers += dc_count
                
                # Store first DC optimizer info
                if not first_dc_optimizer_id and dc_optimizer.get('id'):
                    first_dc_optimizer_id = dc_optimizer.get('id', '')
                    first_dc_optimizer_name = dc_optimizer.get('name', '')
                    qb_record[63] = {'value': first_dc_optimizer_id}
                    qb_record[64] = {'value': first_dc_optimizer_name}
        
        # Set module fields
        qb_record[46] = {'value': first_module_name}
        qb_record[61] = {'value': first_module_id}
        qb_record[62] = {'value': first_module_rating}
        qb_record[47] = {'value': first_module_rating}
        
        # Set panel counts with validation
        qb_record[20] = {'value': total_modules}      # Total modules
        qb_record[30] = {'value': total_modules}      # Duplicate total field
        qb_record[23] = {'value': portrait_count}     # Portrait panels - FIXED
        qb_record[24] = {'value': landscape_count}    # Landscape panels - FIXED
        
        # Set totals for DC optimizers and microinverters
        if total_dc_optimizers > 0:
            qb_record[71] = {'value': total_dc_optimizers}  # DC Optimizer Quantity
            logger.info(f"Total DC Optimizers across all arrays: {total_dc_optimizers}")
        
        if total_microinverters > 0:
            qb_record[59] = {'value': total_microinverters}  # Microinverter count
            logger.info(f"Total Microinverters across all arrays: {total_microinverters}")
        
        # Log the counts for debugging
        logger.info(f"Panel counts - Total: {total_modules}, Portrait: {portrait_count}, Landscape: {landscape_count}")
        
        # Validate counts
        if portrait_count + landscape_count != total_modules:
            logger.warning(f"Panel count mismatch! Total: {total_modules}, Portrait: {portrait_count}, Landscape: {landscape_count}")
        
        # Calculate average solar access and TSRF
        solar_access_sum = 0
        tsrf_sum = 0
        arrays_with_shading = 0
        
        for array in arrays:
            shading = array.get('shading', {})
            if shading:
                solar_access = shading.get('solar_access', {}).get('annual')
                tsrf = shading.get('total_solar_resource_fraction', {}).get('annual')
                
                if solar_access is not None:
                    solar_access_sum += solar_access
                    arrays_with_shading += 1
                
                if tsrf is not None:
                    tsrf_sum += tsrf
        
        if arrays_with_shading > 0:
            qb_record[65] = {'value': round(solar_access_sum / arrays_with_shading, 2)}
            qb_record[67] = {'value': round(tsrf_sum / arrays_with_shading, 2)}
        
        # Store raw arrays data (truncated)
        qb_record[29] = {'value': json.dumps(arrays)[:1000]}
    
    # Process string inverters
    string_inverters = design.get('string_inverters', [])
    if string_inverters:
        qb_record[25] = {'value': len(string_inverters)}
        if string_inverters:
            inv = string_inverters[0]
            qb_record[48] = {'value': inv.get('name', '')}
            qb_record[54] = {'value': inv.get('id', '')}
            qb_record[56] = {'value': inv.get('rated_power', 0)}
    
    # Process Bill of Materials - COMPREHENSIVE with all fields
    bom = design.get('bill_of_materials', [])
    if bom:
        qb_record[44] = {'value': len(bom)}  # BOM Lines Count
        
        # Initialize all BOM quantities to track totals
        module_qty = 0
        inverter_qty = 0
        microinverter_qty = 0
        dc_optimizer_qty = 0
        battery_qty = 0
        combiner_qty = 0
        disconnect_qty = 0
        racking_qty = 0
        
        for item in bom:
            ct = item.get('component_type')
            sku = item.get('sku', '')
            mfg = item.get('manufacturer_name', '')
            qty = item.get('quantity', 0)
            
            if ct == 'modules':
                module_qty = qty
                qb_record[80] = {'value': sku if sku else 'N/A'}  # Module SKU
                qb_record[68] = {'value': mfg if mfg else 'N/A'}  # Module Manufacturer
            elif ct == 'inverters':
                inverter_qty = qty
                qb_record[81] = {'value': sku if sku else 'N/A'}  # Inverter SKU
                qb_record[69] = {'value': mfg if mfg else 'N/A'}  # Inverter Manufacturer
            elif ct == 'microinverters':
                microinverter_qty = qty
                qb_record[83] = {'value': sku if sku else 'N/A'}  # Microinverter SKU
                qb_record[72] = {'value': mfg if mfg else 'N/A'}  # Microinverter Manufacturer
                # Only update count if not already set from arrays
                if 27 not in qb_record:
                    qb_record[27] = {'value': qty}
            elif ct == 'dc_optimizers':
                dc_optimizer_qty = qty
                qb_record[82] = {'value': sku if sku else 'N/A'}  # DC Optimizer SKU
                qb_record[70] = {'value': mfg if mfg else 'N/A'}  # DC Optimizer Manufacturer
                # Field 26 might be different from field 71 - check your QuickBase setup
                if 26 not in qb_record:
                    qb_record[26] = {'value': qty}
            elif ct == 'batteries':
                battery_qty = qty
                qb_record[85] = {'value': sku if sku else 'N/A'}  # Battery SKU
            elif ct == 'combiner_boxes':
                combiner_qty = qty
                qb_record[86] = {'value': sku if sku else 'N/A'}  # Combiner Box SKU
                qb_record[36] = {'value': qty}  # Combiner Qty
            elif ct == 'disconnects':
                disconnect_qty = qty
                qb_record[87] = {'value': sku if sku else 'N/A'}  # Disconnects SKU
                qb_record[37] = {'value': qty}  # Disconnects Qty
            elif ct == 'racking_components' or ct == 'racking':
                racking_qty += qty  # Accumulate all racking components
                if not qb_record.get(84):  # Only set first racking SKU
                    qb_record[84] = {'value': sku if sku else 'N/A'}  # Racking Component SKU
        
        # Set racking total
        if racking_qty > 0:
            qb_record[32] = {'value': racking_qty}  # Racking Attachments Qty
        
        # Set defaults for any BOM fields not found
        if 80 not in qb_record: qb_record[80] = {'value': 'N/A'}  # Module SKU
        if 81 not in qb_record: qb_record[81] = {'value': 'N/A'}  # Inverter SKU
        if 82 not in qb_record: qb_record[82] = {'value': 'N/A'}  # DC Optimizer SKU
        if 83 not in qb_record: qb_record[83] = {'value': 'N/A'}  # Microinverter SKU
        if 84 not in qb_record: qb_record[84] = {'value': 'N/A'}  # Racking SKU
        if 85 not in qb_record: qb_record[85] = {'value': 'N/A'}  # Battery SKU
        if 86 not in qb_record: qb_record[86] = {'value': 'N/A'}  # Combiner SKU
        if 87 not in qb_record: qb_record[87] = {'value': 'N/A'}  # Disconnects SKU
        
        if 32 not in qb_record: qb_record[32] = {'value': 0}  # Racking Qty
        if 36 not in qb_record: qb_record[36] = {'value': 0}  # Combiner Qty
        if 37 not in qb_record: qb_record[37] = {'value': 0}  # Disconnects Qty
        
        qb_record[73] = {'value': json.dumps(bom)[:10000]}  # Full BOM JSON
    else:
        # No BOM data - set all BOM fields to defaults
        qb_record[44] = {'value': 0}   # BOM Lines Count
        qb_record[68] = {'value': 'N/A'}  # Module Manufacturer
        qb_record[69] = {'value': 'N/A'}  # Inverter Manufacturer
        qb_record[70] = {'value': 'N/A'}  # DC Optimizer Manufacturer
        qb_record[72] = {'value': 'N/A'}  # Microinverter Manufacturer
        qb_record[73] = {'value': '[]'}   # BOM JSON
        qb_record[80] = {'value': 'N/A'}  # Module SKU
        qb_record[81] = {'value': 'N/A'}  # Inverter SKU
        qb_record[82] = {'value': 'N/A'}  # DC Optimizer SKU
        qb_record[83] = {'value': 'N/A'}  # Microinverter SKU
        qb_record[84] = {'value': 'N/A'}  # Racking SKU
        qb_record[85] = {'value': 'N/A'}  # Battery SKU
        qb_record[86] = {'value': 'N/A'}  # Combiner SKU
        qb_record[87] = {'value': 'N/A'}  # Disconnects SKU
        qb_record[32] = {'value': 0}      # Racking Qty
        qb_record[36] = {'value': 0}      # Combiner Qty
        qb_record[37] = {'value': 0}      # Disconnects Qty
    
    # Store full design data (truncated)
    qb_record[10] = {'value': json.dumps(design_data)[:10000]}
    
    # Set defaults for any fields that weren't set
    # This ensures all fields have values (0 for numeric, "N/A" for text)
    
    # Customer field default
    if 91 not in qb_record:
        qb_record[91] = {'value': 'N/A'}
    
    # Numeric fields that should default to 0
    numeric_defaults = {
        19: 0,  # Arrays Count
        20: 0,  # Modules Total
        21: 0,  # System DC kW
        22: 0,  # System AC kW
        23: 0,  # Portrait Modules
        24: 0,  # Landscape Modules
        25: 0,  # Inverters Qty
        26: 0,  # DC Optimizer count (different context)
        27: 0,  # Microinverter count
        28: 0,  # Wire Length Ft (if available from Aurora)
        30: 0,  # Modules Qty (duplicate)
        31: 0,  # Rails Length Ft
        32: 0,  # Racking Attachments Qty
        33: 0,  # Conduit Length Ft
        34: 0,  # Breakers Qty
        35: 0,  # Grounding Lugs Qty
        36: 0,  # Combiners Qty
        37: 0,  # Disconnects Qty
        38: 0,  # Base Price
        44: 0,  # BOM Lines Count
        47: 0,  # Module Wattage
        51: 0,  # Annual Production
        56: 0,  # String Inverter Rated Power
        59: 0,  # Microinverter Count
        60: 0,  # Microinverter Rated Power
        62: 0,  # Module STC
        65: 0,  # Solar Access Annual
        67: 0,  # TSRF Value
        71: 0,  # DC Optimizer Quantity
    }
    
    for field_id, default_value in numeric_defaults.items():
        if field_id not in qb_record:
            qb_record[field_id] = {'value': default_value}
    
    # Text fields that should default to "N/A"
    text_defaults = {
        12: 'N/A',  # Field 12
        13: 'N/A',  # Field 13 
        14: 'N/A',  # Field 14
        15: 'N/A',  # Field 15
        29: '[]',   # Arrays Summary (JSON)
        39: 'N/A',  # Adders JSON
        40: 'N/A',  # Incentives JSON
        46: 'N/A',  # Module Model
        48: 'N/A',  # Inverter Model
        49: 'N/A',  # MLPE Model
        50: 'N/A',  # MLPE Qty
        52: '0',    # Annual Offset
        53: '[]',   # Monthly Production
        54: 'N/A',  # String Inverter ID
        57: 'N/A',  # Microinverter ID
        58: 'N/A',  # Microinverter Name
        61: 'N/A',  # Module ID
        63: 'N/A',  # DC Optimizer ID
        64: 'N/A',  # DC Optimizer Name
        66: '[]',   # Solar Access Monthly
        68: 'N/A',  # Module Manufacturer
        69: 'N/A',  # String Inverter Manufacturer
        70: 'N/A',  # DC Optimizer Manufacturer
        72: 'N/A',  # Microinverter Manufacturer
        73: '[]',   # BOM JSON
        74: 'N/A',  # Shading JSON
        75: 'N/A',  # Roof Metrics
        76: 'N/A',  # Other BOM JSON
        77: 'N/A',  # Roof JSON
        78: 'N/A',  # Pricing JSON
        79: 'N/A',  # Cost Payload JSON
        80: 'N/A',  # Module SKU
        81: 'N/A',  # Inverter SKU
        82: 'N/A',  # DC Optimizer SKU
        83: 'N/A',  # Microinverter SKU
        84: 'N/A',  # Racking SKU
        85: 'N/A',  # Battery SKU
        86: 'N/A',  # Combiner Box SKU
        87: 'N/A',  # Disconnects SKU
        88: 'N/A',  # Version
        89: 'N/A',  # Hash/Idempotency Key
        90: 'N/A',  # Error Message
    }
    
    for field_id, default_value in text_defaults.items():
        if field_id not in qb_record:
            qb_record[field_id] = {'value': default_value}
    
    # MLPE Type field (based on what's present)
    mlpe_type = 'N/A'
    # Check if we have DC optimizers or microinverters (use the variables we already have)
    if total_dc_optimizers > 0:  # DC Optimizers
        mlpe_type = 'DC Optimizer'
    elif total_microinverters > 0:  # Microinverters
        mlpe_type = 'Microinverter'
    # Assuming field 49 is MLPE Type - UPDATE IF DIFFERENT
    if 49 not in qb_record or qb_record[49]['value'] == 'N/A':
        qb_record[49] = {'value': mlpe_type}  
    
    # Processing Status (if not set)
    if 17 not in qb_record:
        qb_record[17] = {'value': 'Completed'}
    
    return qb_record

@app.route('/', methods=['GET'])
def home():
    return jsonify({
        'service': 'Aurora Solar Webhook Server',
        'status': 'running',
        'endpoints': {
            'webhook': '/webhook',
            'health': '/health',
            'test': '/test'
        }
    })

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'healthy'})

@app.route('/webhook', methods=['GET', 'POST'])
def webhook():
    # Aurora sends webhook as URL parameters
    project_id = request.args.get('project_id')
    design_id = request.args.get('design_id')
    stage = request.args.get('stage')
    source = request.args.get('source')
    
    logger.info(f"Webhook received - Project: {project_id}, Design: {design_id}, Stage: {stage}, Source: {source}")
    
    # Check if stage is "installed"
    if stage and stage.lower() == 'installed' and design_id:
        logger.info(f"Processing installed design {design_id}")
        
        process_data = {
            'design_id': design_id,
            'project_id': project_id,
            'stage': stage,
            'source': source
        }
        
        thread = threading.Thread(target=process_single_design, args=(process_data,))
        thread.daemon = True
        thread.start()
        
        return jsonify({'status': 'accepted', 'message': 'Processing design'}), 200
    else:
        logger.info(f"Skipping - Stage is '{stage}', not 'installed'")
        return jsonify({'status': 'skipped', 'stage': stage}), 200

def process_single_design(data: Dict):
    """Process a single design that was marked as installed"""
    try:
        design_id = data.get('design_id')
        project_id = data.get('project_id')
        logger.info(f"Starting to process design {design_id}, project {project_id}")
        
        aurora_client = AuroraSolarClient()
        qb_client = QuickbaseClient()
        
        # Fetch design summary
        logger.info(f"Fetching design summary for {design_id}")
        design_data = aurora_client.get_design_summary(design_id)
        
        # Fetch project details for customer info
        project_data = None
        if project_id:
            logger.info(f"Fetching project details for {project_id}")
            project_data = aurora_client.get_project(project_id)
        elif design_data and design_data.get('design', {}).get('project_id'):
            # If project_id wasn't in webhook, try to get it from design
            project_id = design_data.get('design', {}).get('project_id')
            logger.info(f"Fetching project details for {project_id} (from design data)")
            project_data = aurora_client.get_project(project_id)
        
        if design_data:
            logger.info(f"Got design data, transforming for Quickbase")
            
            # Add project data to the payload
            if project_data:
                design_data['project'] = project_data.get('project', project_data)
            
            qb_record = transform_data(design_data)
            
            logger.info(f"Sending to Quickbase table {QUICKBASE_CONFIG['table_id']}")
            success = qb_client.upsert_record(qb_record)
            
            if success:
                logger.info(f"✓ Successfully synced design {design_id} to Quickbase")
            else:
                logger.error(f"Failed to sync to Quickbase")
        else:
            logger.error(f"Could not fetch design data for {design_id}")
            
    except Exception as e:
        logger.error(f"Error processing design: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())

@app.route('/test', methods=['GET'])
def test():
    """Test endpoint to manually trigger a sync"""
    design_id = request.args.get('design_id')
    if design_id:
        process_data = {
            'design_id': design_id,
            'project_id': 'test',
            'stage': 'installed',
            'source': 'test'
        }
        thread = threading.Thread(target=process_single_design, args=(process_data,))
        thread.daemon = True
        thread.start()
        return jsonify({'status': 'test started', 'design_id': design_id}), 200
    else:
        return jsonify({'error': 'Please provide design_id parameter'}), 400

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port)
