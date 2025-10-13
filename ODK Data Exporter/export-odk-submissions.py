import requests
import os
import configparser
from pathlib import Path
from tqdm import tqdm

class ODKSubmissionDownloader:
    def __init__(self, config_path='config.ini'):
        self.config = self._load_config(config_path)
        self.session = requests.Session()
        self.token = None
        
    def _load_config(self, config_path):
        """Load configuration from INI file"""
        if not Path(config_path).exists():
            raise FileNotFoundError(f"Configuration file {config_path} not found")
            
        config = configparser.ConfigParser()
        config.read(config_path)
        
        # Validate required sections
        if not all(section in config for section in ['odk', 'settings']):
            raise ValueError("Configuration file missing required sections")
            
        return config

    def authenticate(self):
        """Authenticate with ODK Central server"""
        auth_url = f"{self.config['odk']['server_url']}/v1/sessions"
        response = self.session.post(
            auth_url,
            json={
                "email": self.config['odk']['email'],
                "password": self.config['odk']['password']
            },
            headers={"Content-Type": "application/json"},
            timeout=int(self.config['settings']['timeout'])
        )
        response.raise_for_status()
        self.token = response.json()["token"]
        self.session.headers.update({"Authorization": f"Bearer {self.token}"})

    def get_all_projects(self):
        """Get all projects on the server"""
        projects_url = f"{self.config['odk']['server_url']}/v1/projects"
        response = self.session.get(
            projects_url,
            timeout=int(self.config['settings']['timeout'])
        )
        response.raise_for_status()
        return response.json()

    def get_project_forms(self, project_id):
        """Retrieve all forms for a given project"""
        forms_url = f"{self.config['odk']['server_url']}/v1/projects/{project_id}/forms"
        response = self.session.get(
            forms_url,
            timeout=int(self.config['settings']['timeout'])
        )
        response.raise_for_status()
        return response.json()

    def download_form_submissions(self, project_name, project_id, form_id, form_name):
        """Download submissions for a single form"""
        format = self.config['settings']['preferred_format']
        download_url = f"{self.config['odk']['server_url']}/v1/projects/{project_id}/forms/{form_id}/submissions.{format}"

        
        for attempt in range(int(self.config['settings']['max_retries'])):
            try:
                response = self.session.get(
                    download_url,
                    stream=True,
                    timeout=int(self.config['settings']['timeout'])
                )
                response.raise_for_status()

                # Build output path: output_dir/project_name/
                output_dir = os.path.join(self.config['odk']['output_dir'], self._sanitize(project_name))
                os.makedirs(output_dir, exist_ok=True)

                # File name
                safe_form_name = self._sanitize(form_name)
                filename = f"{safe_form_name}_submissions.{format}"
                filepath = os.path.join(output_dir, filename)
                
                with open(filepath, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                
                return True, filename
                
            except requests.exceptions.RequestException as e:
                if attempt == int(self.config['settings']['max_retries']) - 1:
                    return False, str(e)
                time.sleep(2 ** attempt)  # exponential backoff

    def _sanitize(self, name):
        return "".join(c if c.isalnum() else "_" for c in name)

    def run(self):
        """Main execution method"""
        try:
            self.authenticate()
            projects = self.get_all_projects()
            print(f"✓ Found {len(projects)} projects")

            for project in tqdm(projects, desc="Processing projects"):
                project_id = project["id"]
                project_name = project.get("name", f"project_{project_id}")
                print(f"\n→ Project: {project_name} (ID: {project_id})")

                forms = self.get_project_forms(project_id)
                if not forms:
                    print("  ⚠ No forms in this project")
                    continue

                for form in forms:
                    form_id = form["xmlFormId"]
                    form_name = form.get("name", form_id)

                    print(f"  ↓ Downloading form: {form_name}")
                    success, result = self.download_form_submissions(project_name, project_id, form_id, form_name)
                    if success:
                        print(f"    ✓ Saved to: {result}")
                    else:
                        print(f"    ✗ Failed to download {form_name}: {result}")
                    
        except Exception as e:
            print(f"! Critical error: {str(e)}")
        finally:
            print("Download process completed")

if __name__ == "__main__":
    downloader = ODKSubmissionDownloader()
    downloader.run()