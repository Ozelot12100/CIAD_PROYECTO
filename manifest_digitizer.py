#!/usr/bin/env python3
"""
CIAD Manifest Digitization Script
=================================

This script processes the rawdata.csv file and creates individual manifest entries
just like the CreateManifest.tsx component does - one inspection at a time.

Each row in rawdata.csv represents one physical manifest document that needs to be digitized.
"""

import pandas as pd
import requests
import json
import re
from datetime import datetime, timezone
import logging
from typing import Dict, List, Optional, Any
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CIADManifestDigitizer:
    """Digitizes physical manifest documents into CIAD system"""
    
    def __init__(self, base_url: str = "http://localhost:1234"):
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        
        # Cache for entity lookups
        self.vessels_cache = {}
        self.persons_cache = {}
        self.waste_types_cache = {}
        self.system_users_cache = {}
        
        # Statistics
        self.stats = {
            'processed': 0,
            'successful': 0,
            'failed': 0,
            'skipped': 0
        }
    
    def _make_request(self, method: str, endpoint: str, data: Optional[Dict] = None) -> Optional[Dict]:
        """Make HTTP request to API"""
        url = f"{self.base_url}/api{endpoint}"
        
        try:
            if method.upper() == 'GET':
                response = self.session.get(url)
            elif method.upper() == 'POST':
                response = self.session.post(url, json=data)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            response.raise_for_status()
            return response.json() if response.content else {}
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {method} {url} - {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response: {e.response.text}")
            return None
    
    def load_entities_cache(self):
        """Load all entities needed for manifest creation"""
        logger.info("Loading entities cache...")
        
        # Load vessels
        vessels_response = self._make_request('GET', '/vessels/0/1000')
        if vessels_response and vessels_response.get('data'):
            for vessel in vessels_response['data']:
                # Handle both ID and id cases for vessel names
                vessel_name = vessel['VesselName'].lower().strip()
                self.vessels_cache[vessel_name] = vessel
        
        # Load persons
        persons_response = self._make_request('GET', '/persons/0/5000')
        if persons_response and persons_response.get('data'):
            for person in persons_response['data']:
                # Handle both ID and id cases
                person_name = person['Name'].lower().strip()
                self.persons_cache[person_name] = person
        
        # Load waste types
        waste_types_response = self._make_request('GET', '/wasteTypes/0/100')
        if waste_types_response and waste_types_response.get('data'):
            for waste_type in waste_types_response['data']:
                self.waste_types_cache[waste_type['typeName'].lower().strip()] = waste_type
        
        # Load system users
        system_users_response = self._make_request('GET', '/systemUsers/0/100')
        if system_users_response and system_users_response.get('data'):
            for user in system_users_response['data']:
                self.system_users_cache[user['FullName'].lower().strip()] = user
        
        logger.info(f"Loaded cache: {len(self.vessels_cache)} vessels, {len(self.persons_cache)} persons, "
                   f"{len(self.waste_types_cache)} waste types, {len(self.system_users_cache)} system users")
    
    def find_or_create_vessel(self, vessel_name: str) -> Optional[Dict]:
        """Find vessel by name or return None if not found"""
        vessel_key = vessel_name.lower().strip()
        if vessel_key in self.vessels_cache:
            return self.vessels_cache[vessel_key]
        
        logger.warning(f"Vessel '{vessel_name}' not found in cache")
        return None
    
    def find_or_create_person(self, person_name: str, person_type: str = "Crew") -> Optional[Dict]:
        """Find person by name or return None if not found"""
        if not person_name or person_name.strip() == "":
            return None
            
        person_key = person_name.lower().strip()
        if person_key in self.persons_cache:
            return self.persons_cache[person_key]
        
        logger.warning(f"Person '{person_name}' not found in cache")
        return None
    
    def get_waste_type_by_name(self, waste_type_name: str) -> Optional[Dict]:
        """Get waste type by name"""
        waste_key = waste_type_name.lower().strip()
        if waste_key in self.waste_types_cache:
            return self.waste_types_cache[waste_key]
        
        logger.warning(f"Waste type '{waste_type_name}' not found in cache")
        return None
    
    def get_default_inspector(self) -> Dict:
        """Get default system user (inspector)"""
        if self.system_users_cache:
            # Return first available system user
            return list(self.system_users_cache.values())[0]
        
        # If no system users exist, return a default structure
        return {'ID': 1, 'FullName': 'Default Inspector'}
    
    def create_compliance_manifest(self, manifest_data: Dict) -> bool:
        """Create a compliance manifest entry (like CreateManifest.tsx does)"""
        try:
            # 1. Find vessel
            vessel = self.find_or_create_vessel(manifest_data['vessel_name'])
            if not vessel:
                logger.error(f"Cannot create manifest: vessel '{manifest_data['vessel_name']}' not found")
                return False
            
            # 2. Find signers (captain and chef)
            captain = self.find_or_create_person(manifest_data['captain_name'], "Captain")
            chef = self.find_or_create_person(manifest_data['chef_name'], "Chef")
            
            # Collect signers (skip if not found)
            signers_ids = []
            if captain:
                captain_id = captain.get('ID') or captain.get('id')
                if captain_id:
                    signers_ids.append(captain_id)
            if chef:
                chef_id = chef.get('ID') or chef.get('id')
                if chef_id:
                    signers_ids.append(chef_id)
            
            # If no signers found, use default (required by API)
            if not signers_ids:
                logger.warning(f"No signers found for manifest, using default")
                signers_ids = [1]  # Default signer
            
            # 3. Get inspector
            inspector = self.get_default_inspector()
            inspector_id = inspector.get('ID') or inspector.get('id')
            
            # 4. Create compliance record
            compliance_data = {
                "vesselId": vessel.get('ID') or vessel.get('id'),
                "inspectionDate": manifest_data['inspection_date'],
                "observations": manifest_data['observations'],
                "systemUserId": inspector_id,
                "signersIds": signers_ids
            }
            
            logger.info(f"Creating compliance for vessel '{manifest_data['vessel_name']}'...")
            compliance_response = self._make_request('POST', '/compliances', compliance_data)
            
            if not compliance_response or not compliance_response.get('success'):
                logger.error(f"Failed to create compliance: {compliance_response}")
                return False
            
            compliance = compliance_response.get('data')
            if not compliance:
                logger.error("No compliance data returned")
                return False
            
            logger.info(f"Created compliance ID: {compliance.get('id') or compliance.get('ID')}")
            
            # Get compliance ID (handle both lowercase and uppercase)
            compliance_id = compliance.get('id') or compliance.get('ID')
            if not compliance_id:
                logger.error("No compliance ID found in response")
                return False
            
            # 5. Create waste records
            waste_records_created = 0
            
            # Oil waste
            if manifest_data['oil_used'] > 0:
                oil_waste_type = self.get_waste_type_by_name("Aceite")  # Use actual waste type name
                if oil_waste_type:
                    waste_data = {
                        "vesselId": vessel.get('ID') or vessel.get('id'),
                        "wasteTypeId": oil_waste_type.get('id'),
                        "quantityGenerated": manifest_data['oil_used'],
                        "generationDate": manifest_data['inspection_date'],
                        "complianceId": compliance_id
                    }
                    
                    waste_response = self._make_request('POST', '/wastes', waste_data)
                    if waste_response and waste_response.get('success'):
                        waste_records_created += 1
                        logger.info(f"Created oil waste record: {manifest_data['oil_used']} liters")
            
            # Oil filters waste
            if manifest_data['oil_filters_used'] > 0:
                oil_filters_waste_type = self.get_waste_type_by_name("Filtros de Aceite")  # Use actual waste type name
                if oil_filters_waste_type:
                    waste_data = {
                        "vesselId": vessel.get('ID') or vessel.get('id'),
                        "wasteTypeId": oil_filters_waste_type.get('id'),
                        "quantityGenerated": manifest_data['oil_filters_used'],
                        "generationDate": manifest_data['inspection_date'],
                        "complianceId": compliance_id
                    }
                    
                    waste_response = self._make_request('POST', '/wastes', waste_data)
                    if waste_response and waste_response.get('success'):
                        waste_records_created += 1
                        logger.info(f"Created oil filters waste record: {manifest_data['oil_filters_used']} units")
            
            # Diesel filters waste
            if manifest_data['diesel_filters_used'] > 0:
                diesel_filters_waste_type = self.get_waste_type_by_name("Filtros Diesel")  # Use actual waste type name
                if diesel_filters_waste_type:
                    waste_data = {
                        "vesselId": vessel.get('ID') or vessel.get('id'),
                        "wasteTypeId": diesel_filters_waste_type.get('id'),
                        "quantityGenerated": manifest_data['diesel_filters_used'],
                        "generationDate": manifest_data['inspection_date'],
                        "complianceId": compliance_id
                    }
                    
                    waste_response = self._make_request('POST', '/wastes', waste_data)
                    if waste_response and waste_response.get('success'):
                        waste_records_created += 1
                        logger.info(f"Created diesel filters waste record: {manifest_data['diesel_filters_used']} units")
            
            # Junk/General waste
            if manifest_data['junk_reported'] > 0:
                junk_waste_type = self.get_waste_type_by_name("Desechos Solidos")  # Use actual waste type name
                
                if junk_waste_type:
                    waste_data = {
                        "vesselId": vessel.get('ID') or vessel.get('id'),
                        "wasteTypeId": junk_waste_type.get('id'),
                        "quantityGenerated": manifest_data['junk_reported'],
                        "generationDate": manifest_data['inspection_date'],
                        "complianceId": compliance_id
                    }
                    
                    waste_response = self._make_request('POST', '/wastes', waste_data)
                    if waste_response and waste_response.get('success'):
                        waste_records_created += 1
                        logger.info(f"Created general waste record: {manifest_data['junk_reported']} kg")
            
            logger.info(f"Successfully created manifest with {waste_records_created} waste records")
            return True
            
        except Exception as e:
            logger.error(f"Error creating compliance manifest: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    def clean_numeric_value(self, value_str: str) -> float:
        """Extract numeric value from string that may contain units like 'litros', 'kg', etc."""
        if not value_str or pd.isna(value_str):
            return 0.0
        
        # Convert to string and clean
        value_str = str(value_str).strip()
        
        # Extract just the numeric part using regex
        import re
        numeric_match = re.search(r'(\d+(?:\.\d+)?)', value_str)
        if numeric_match:
            return float(numeric_match.group(1))
        
        return 0.0
    
    def format_date(self, date_str: str) -> str:
        """Format date string to ISO format"""
        try:
            # Handle different date formats
            if '/' in date_str:
                date_obj = datetime.strptime(date_str, '%Y/%m/%d')
            else:
                date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            
            # Add timezone info
            date_obj = date_obj.replace(tzinfo=timezone.utc)
            return date_obj.isoformat()
        except:
            logger.warning(f"Could not parse date: {date_str}, using current date")
            return datetime.now(timezone.utc).isoformat()
    
    def process_raw_data(self, csv_file: str, start_from: int = 0, max_records: int = None) -> Dict:
        """Process raw manifest data CSV file"""
        logger.info(f"Loading raw data from {csv_file}...")
        
        try:
            df = pd.read_csv(csv_file)
            logger.info(f"Loaded {len(df)} manifest records")
            
            # Clean data
            df = df.dropna(how='all')
            
            # Filter out test records
            df = df[~df['Vessel Name'].str.lower().str.contains('test', na=False)]
            
            # Apply slice if specified
            if start_from > 0:
                df = df.iloc[start_from:]
            if max_records:
                df = df.head(max_records)
            
            logger.info(f"Processing {len(df)} records (starting from index {start_from})")
            
            # Load entities cache first
            self.load_entities_cache()
            
            # Process each manifest record
            for index, row in df.iterrows():
                self.stats['processed'] += 1
                
                try:
                    # Parse manifest data
                    manifest_data = {
                        'vessel_name': str(row['Vessel Name']).strip(),
                        'inspection_date': self.format_date(str(row['Date'])),
                        'oil_used': self.clean_numeric_value(row['Oil Used']),
                        'oil_filters_used': self.clean_numeric_value(row['Oil Filters Used']),
                        'diesel_filters_used': self.clean_numeric_value(row['Diesel Filters Used']),
                        'junk_reported': self.clean_numeric_value(row['Junk Reported']),
                        'captain_name': str(row['Captain Full Name']).strip() if pd.notna(row['Captain Full Name']) else "",
                        'chef_name': str(row['Chef Full Name']).strip() if pd.notna(row['Chef Full Name']) else "",
                        'observations': f"Digitized from physical manifest ID: {row['ID']}"
                    }
                    
                    # Skip if no waste reported
                    total_waste = (manifest_data['oil_used'] + manifest_data['oil_filters_used'] + 
                                 manifest_data['diesel_filters_used'] + manifest_data['junk_reported'])
                    
                    if total_waste == 0:
                        logger.info(f"Skipping manifest for '{manifest_data['vessel_name']}' - no waste reported")
                        self.stats['skipped'] += 1
                        continue
                    
                    # Create the manifest
                    success = self.create_compliance_manifest(manifest_data)
                    
                    if success:
                        self.stats['successful'] += 1
                        logger.info(f"✓ Processed manifest {self.stats['processed']}/{len(df)} for vessel '{manifest_data['vessel_name']}'")
                    else:
                        self.stats['failed'] += 1
                        logger.error(f"✗ Failed to process manifest {self.stats['processed']}/{len(df)} for vessel '{manifest_data['vessel_name']}'")
                    
                    # Progress report every 50 records
                    if self.stats['processed'] % 50 == 0:
                        logger.info(f"Progress: {self.stats['processed']}/{len(df)} processed, "
                                  f"{self.stats['successful']} successful, {self.stats['failed']} failed, "
                                  f"{self.stats['skipped']} skipped")
                
                except Exception as e:
                    self.stats['failed'] += 1
                    logger.error(f"Error processing manifest {self.stats['processed']}: {e}")
                    continue
            
            return self.stats
            
        except Exception as e:
            logger.error(f"Failed to process raw data: {e}")
            return self.stats

    def create_person_batch(self, name: str, person_type_id: int, contact_info: str = "N/A") -> Optional[Dict]:
        """Create a person via API"""
        data = {
            "name": name,
            "personTypeId": person_type_id,
            "contactInfo": contact_info
        }
        
        response = self._make_request('POST', '/persons', data)
        if response and response.get('success'):
            person = response.get('data')
            person_id = person.get('ID') or person.get('id')
            logger.info(f"✓ Created person: {name} (ID: {person_id})")
            return person
        else:
            logger.error(f"✗ Failed to create person: {name}")
            return None
    
    def create_vessel_batch(self, vessel_name: str, vessel_type: str, owner_id: int) -> Optional[Dict]:
        """Create a vessel via API"""
        data = {
            "vesselName": vessel_name,
            "vesselType": vessel_type,
            "ownerId": owner_id
        }
        
        response = self._make_request('POST', '/vessels', data)
        if response and response.get('success'):
            vessel = response.get('data')
            vessel_id = vessel.get('ID') or vessel.get('id')
            logger.info(f"✓ Created vessel: {vessel_name} (ID: {vessel_id})")
            return vessel
        else:
            logger.error(f"✗ Failed to create vessel: {vessel_name}")
            return None
    
    def setup_base_entities(self, csv_file: str, max_records: int = None) -> Dict:
        """Phase 1: Create all needed persons and vessels from the CSV data"""
        logger.info("="*60)
        logger.info("PHASE 1: CREATING BASE ENTITIES")
        logger.info("="*60)
        
        try:
            df = pd.read_csv(csv_file)
            logger.info(f"Loaded {len(df)} manifest records")
            
            # Clean data
            df = df.dropna(how='all')
            df = df[~df['Vessel Name'].str.lower().str.contains('test', na=False)]
            
            if max_records:
                df = df.head(max_records)
                
            logger.info(f"Processing {len(df)} records for entity creation")
            
            # Person types: 1=Cocinero (Chef), 2=Motorista (Captain), 3=Propietario (Owner)
            COCINERO_TYPE = 1
            MOTORISTA_TYPE = 2
            PROPIETARIO_TYPE = 3
            
            # Collections for unique entities
            unique_vessels = set()
            unique_captains = set()
            unique_chefs = set()
            
            # Extract unique entities from data
            for index, row in df.iterrows():
                vessel_name = str(row['Vessel Name']).strip()
                captain_name = str(row['Captain Full Name']).strip()
                chef_name = str(row['Chef Full Name']).strip()
                
                if vessel_name and vessel_name != 'nan':
                    unique_vessels.add(vessel_name)
                if captain_name and captain_name != 'nan':
                    unique_captains.add(captain_name)
                if chef_name and chef_name != 'nan':
                    unique_chefs.add(chef_name)
            
            logger.info(f"Found {len(unique_vessels)} unique vessels")
            logger.info(f"Found {len(unique_captains)} unique captains")  
            logger.info(f"Found {len(unique_chefs)} unique chefs")
            
            # Phase 1A: Create Unknown Owner (for all vessels)
            logger.info("\n--- Creating Unknown Owner ---")
            unknown_owner = self.create_person_batch("Unknown Owner", PROPIETARIO_TYPE, "Default vessel owner")
            if not unknown_owner:
                logger.error("Failed to create unknown owner - aborting")
                return {'error': 'Failed to create unknown owner'}
            
            unknown_owner_id = unknown_owner.get('ID') or unknown_owner.get('id')
            
            # Phase 1B: Create all Captains (Motoristas)
            logger.info(f"\n--- Creating {len(unique_captains)} Captains ---")
            created_captains = 0
            for captain_name in unique_captains:
                if self.create_person_batch(captain_name, MOTORISTA_TYPE, "Captain"):
                    created_captains += 1
            
            # Phase 1C: Create all Chefs (Cocineros)
            logger.info(f"\n--- Creating {len(unique_chefs)} Chefs ---")
            created_chefs = 0
            for chef_name in unique_chefs:
                if self.create_person_batch(chef_name, COCINERO_TYPE, "Chef"):
                    created_chefs += 1
            
            # Phase 1D: Create all Vessels
            logger.info(f"\n--- Creating {len(unique_vessels)} Vessels ---")
            created_vessels = 0
            for vessel_name in unique_vessels:
                if self.create_vessel_batch(vessel_name, "Boat", unknown_owner_id):
                    created_vessels += 1
            
            logger.info("\n" + "="*60)
            logger.info("PHASE 1 COMPLETE - BASE ENTITIES CREATED")
            logger.info("="*60)
            logger.info(f"Created: 1 unknown owner, {created_captains} captains, {created_chefs} chefs, {created_vessels} vessels")
            
            return {
                'unknown_owner': 1,
                'captains': created_captains,
                'chefs': created_chefs,
                'vessels': created_vessels,
                'total_persons': 1 + created_captains + created_chefs,
                'total_vessels': created_vessels
            }
            
        except Exception as e:
            logger.error(f"Failed to setup base entities: {e}")
            return {'error': str(e)}

def main():
    print("CIAD Manifest Digitization - Batch Processing")
    print("=" * 50)
    print()
    
    # Initialize digitizer
    digitizer = CIADManifestDigitizer(base_url="http://localhost:1234")
    
    print("Choose processing mode:")
    print("1. Setup base entities only (vessels, persons)")
    print("2. Process compliance manifests only")
    print("3. Full process (entities + manifests)")
    print()
    
    mode = input("Enter choice (1, 2, or 3): ").strip()
    
    if mode == "1":
        # Phase 1: Create base entities
        print("\nStarting Phase 1: Creating base entities...")
        print("This will create all unique vessels, captains, chefs, and owners from the data")
        print()
        
        max_records = input("Max records to process (press Enter for all): ").strip()
        max_records = int(max_records) if max_records else None
        
        entity_stats = digitizer.setup_base_entities('rawdata.csv', max_records)
        
        if 'error' in entity_stats:
            print(f"❌ Error: {entity_stats['error']}")
        else:
            print("\n✅ Base entities created successfully!")
            print(f"   - Persons: {entity_stats['total_persons']}")
            print(f"   - Vessels: {entity_stats['total_vessels']}")
            print("\nYou can now run mode 2 to process compliance manifests.")
    
    elif mode == "2":
        # Phase 2: Process compliance manifests
        print("\nStarting Phase 2: Processing compliance manifests...")
        print("This will create compliance records and waste entries")
        print()
        
        start_from = input("Start from record (default 0): ").strip()
        start_from = int(start_from) if start_from else 0
        
        max_records = input("Max records to process (default 50): ").strip()
        max_records = int(max_records) if max_records else 50
        
        stats = digitizer.process_raw_data('rawdata.csv', start_from, max_records)
        
        print("\n" + "="*50)
        print("PHASE 2 COMPLETE - COMPLIANCE MANIFESTS")
        print("="*50)
        print(f"Total processed: {stats['processed']}")
        print(f"Successful: {stats['successful']}")
        print(f"Failed: {stats['failed']}")
        print(f"Skipped: {stats['skipped']}")
        
        if stats['successful'] > 0:
            print("\n✅ Manifests digitized successfully!")
            print("   You can view them in the CIAD frontend.")
    
    elif mode == "3":
        # Full process
        print("\nStarting full process...")
        
        max_records = input("Max records for entities (press Enter for all): ").strip()
        max_records = int(max_records) if max_records else None
        
        # Phase 1: Entities
        entity_stats = digitizer.setup_base_entities('rawdata.csv', max_records)
        
        if 'error' in entity_stats:
            print(f"❌ Error in Phase 1: {entity_stats['error']}")
            return
        
        print(f"\n✅ Phase 1 complete: {entity_stats['total_persons']} persons, {entity_stats['total_vessels']} vessels")
        
        # Phase 2: Compliance manifests
        print("\nStarting Phase 2...")
        manifest_records = input("Max manifest records to process (default 50): ").strip()
        manifest_records = int(manifest_records) if manifest_records else 50
        
        stats = digitizer.process_raw_data('rawdata.csv', 0, manifest_records)
        
        print("\n" + "="*60)
        print("FULL PROCESS COMPLETE")
        print("="*60)
        print(f"Entities: {entity_stats['total_persons']} persons, {entity_stats['total_vessels']} vessels")
        print(f"Manifests: {stats['successful']} successful, {stats['failed']} failed, {stats['skipped']} skipped")
    
    else:
        print("Invalid choice. Please run again and select 1, 2, or 3.")

if __name__ == '__main__':
    main()
