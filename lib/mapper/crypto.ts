import {
  KmsKeyringNode,
  buildClient,
  CommitmentPolicy,
  NodeCachingMaterialsManager,
  getLocalCryptographicMaterialsCache,
} from '@aws-crypto/client-node';

export class Crypto {
  private static _cachedClient: ReturnType<typeof buildClient> | undefined;
  private static _cachedCMM: NodeCachingMaterialsManager | undefined;
  private readonly client: ReturnType<typeof buildClient>;
  private readonly cmm: NodeCachingMaterialsManager;

  constructor() {
    if (!Crypto._cachedClient) {
      Crypto._cachedClient = buildClient({
        commitmentPolicy: CommitmentPolicy.REQUIRE_ENCRYPT_REQUIRE_DECRYPT,
        maxEncryptedDataKeys: 1,
      });
    }

    if (!Crypto._cachedCMM) {
      const masterKeyArn = process.env['MASTER_KEY_ARN'];
      if (!masterKeyArn) {
        throw new Error('Encryption Master Key is not configured');
      }

      Crypto._cachedCMM = new NodeCachingMaterialsManager({
        backingMaterials: new KmsKeyringNode({ generatorKeyId: masterKeyArn }),
        cache: getLocalCryptographicMaterialsCache(500),
        maxAge: 10 * 60 * 1000, //10 Minutes
      });
    }

    this.client = Crypto._cachedClient;
    this.cmm = Crypto._cachedCMM;
  }

  async encrypt(plainText: string): Promise<string> {
    const { result } = await this.client.encrypt(this.cmm, plainText);
    return result.toString('base64');
  }

  async decrypt(cipherText: string): Promise<string> {
    const { plaintext } = await this.client.decrypt(this.cmm, Buffer.from(cipherText, 'base64'));
    return plaintext.toString();
  }
}
