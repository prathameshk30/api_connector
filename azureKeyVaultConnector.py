from os import environ as env
import json
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient

# config
FILE_PATH=""
SERVICE_PRINCIPAL_CONFIG= json.loads(FILE_PATH)

# create class
class azureKeyVaultConnector:

    def __init__(self):

        # Config
        self.TENANT_ID =     SERVICE_PRINCIPAL_CONFIG['azure']['TENANT_ID']
        self.CLIENT_ID =     SERVICE_PRINCIPAL_CONFIG['azure']['CLIENT_ID']
        self.CLIENT_SECRET = SERVICE_PRINCIPAL_CONFIG['azure']['CLIENT_SECRET']

        # Select Key-Vault Name using enviornment
        if mssparkutils.env.getWorkspaceName()[11:15] in ("6346", "6345"):
            self.KEYVAULT_NAME = f"KV-BAU-APP{mssparkutils.env.getWorkspaceName()[11:15]}-N-CUS01"
        else:
            self.KEYVAULT_NAME = f"KV-BAU-APP{mssparkutils.env.getWorkspaceName()[11:15]}-P-CUS01"

        # Set your Key Vault name
        self.KEYVAULT_URL= f"https://{self.KEYVAULT_NAME}.vault.azure.net/"
    
    def _create_azure_client(self):
        # pass Azure client credentials
        self.CREDENTIAL = ClientSecretCredential(
            tenant_id= self.TENANT_ID,
            client_id= self.CLIENT_ID,
            client_secret= self.CLIENT_SECRET
        )

        # Create the SecretClient
        self.client = SecretClient(vault_url= self.KEYVAULT_URL, credential= self.CREDENTIAL )

        return self.client
    
    def get_kv_secrets(self, secret_name):
        try:
            client = self._create_azure_client()
            retrieved_secret = client.get_secret(secret_name)
            return retrieved_secret.value
        except Exception as e:
            print(f"Failed to retrieve secret '{secret_name}': {e}")
            return None

# Test Connector
# connector = azureKeyVaultConnector()
# sfdc_secret = connector.get_kv_secrets("kv-sfdcClientSecret")
# print(sfdc_secret)