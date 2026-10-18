import {
  clusterApiUrl,
    Connection,
    Keypair,
    LAMPORTS_PER_SOL,
    PublicKey,
    Transaction,
  } from "@solana/web3.js";
  import { PumpHelper } from "./pumpHelper/helper";
  import { bs58 } from "@coral-xyz/anchor/dist/cjs/utils/bytes";
  import { BN } from "bn.js";
import dotenv from "dotenv";

dotenv.config();
const connection = new Connection(clusterApiUrl("mainnet-beta"), "confirmed");
const sdk = new PumpHelper(connection);

const signer = Keypair.fromSecretKey(
  bs58.decode(process.env.PRIVATE_KEY!)
);

const GLOBAL_CONFIG = {
    initialized: true,
    authority: new PublicKey("FFWtrEQ4B4PKQoVuHYzZq8FabGkVatYzDpEVHsK5rrhF"),
    feeRecipient: new PublicKey("62qc2CNXwrYqQScmEdiZFFAnJR262PxWEuNQtxfafNgV"),
    initialVirtualTokenReserves: new BN("3cfe347d81000", 16),
    initialVirtualSolReserves: new BN("6fc23ac00", 16),
    initialRealTokenReserves: new BN("2d151fbc57800", 16),
    tokenTotalSupply: new BN("38d7ea4c68000", 16),
    feeBasisPoints: new BN("5f", 16),
    withdrawAuthority: new PublicKey("39azUYFWPz3VHgKCf3VChUwbpURdCHRxjWVowf5jUJjg"),
    enableMigrate: true,
    poolMigrationFee: new BN("e4e1c1", 16),
    creatorFeeBasisPoints: new BN("5", 16),
    feeRecipients: [
      new PublicKey("7VtfL8fvgNfhz17qKRMjzQEXgbdpnHHHQRh54R9jP2RJ"),
      new PublicKey("7hTckgnGnLQR6sdH7YkqFTAA7VwTfYFaZ6EhEsU3saCX"),
      new PublicKey("9rPYyANsfQZw3DnDmKE3YCQF5E8oD89UXoHn9JFEhJUz"),
      new PublicKey("AVmoTthdrX6tKt4nDjco2D775W2YK3sDhxPcMmzUAmTY"),
      new PublicKey("CebN5WGQ4jvEPvsVU4EoHEpgzq1VV7AbicfhtW4xC9iM"),
      new PublicKey("FWsW1xNtWscwNmKv6wVsU1iTzRN6wmmk3MjxRP5tT7hz"),
      new PublicKey("G5UZAVbAf46s7cKWoyKu8kYTip9DGTpbLZ2qa9Aq69dP")
    ],
    setCreatorAuthority: new PublicKey("39azUYFWPz3VHgKCf3VChUwbpURdCHRxjWVowf5jUJjg"),
    adminSetCreatorAuthority: new PublicKey("UqN2p5bAzBqYdHXcgB6WLtuVrdvmy9JSAtgqZb3CMKw")
  } as const; // 'as const' makes it readonly


export const SnipeToken = async(mint:PublicKey,creator:PublicKey) =>{


    const solAmount = 0.001;
    const tokenAmount = 17376518
    // const  =  new PublicKey("BEqEgSgcm9mDg9xjKwLTtx9yyML9DCDuEbkDRN6pNBWY")


    const [tx2, latestBlockhash] = await Promise.all([
        sdk.getBuyTxs(
          //@ts-ignore
          GLOBAL_CONFIG,
          creator,
          mint,
          signer.publicKey,
          100,
          new BN(tokenAmount),
          new BN(solAmount * LAMPORTS_PER_SOL)
        ),
        connection.getLatestBlockhash()
      ]);

  
  
    if (tx2.success ) {
    const transection = new Transaction().add(...tx2.data);
    transection.recentBlockhash = latestBlockhash.blockhash;
    transection.feePayer = signer.publicKey;

  
    const simualtion = await connection.simulateTransaction(transection);
    console.log("here is simulation---", simualtion);

    transection.sign(signer);
    const sign = await connection.sendRawTransaction(transection.serialize());
    console.log("here is sign---", `https://solscan.io/tx/${sign}`);
    }

}


