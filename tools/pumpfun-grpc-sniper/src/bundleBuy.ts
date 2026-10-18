import {
    AddressLookupTableProgram,
    clusterApiUrl,
    Connection,
    Keypair,
    LAMPORTS_PER_SOL,
    PublicKey,
    TransactionMessage,
    VersionedTransaction,
  } from "@solana/web3.js";
  import dotenv from "dotenv";
  import BN from "bn.js";

import { bundleBuyerKeys } from "./buyersKeys";
import { PumpHelper } from "./pumpHelper/helper";
  
  dotenv.config();


  const connection = new Connection(clusterApiUrl("mainnet-beta"), "confirmed");
  const sdk = new PumpHelper(connection);


const bundleBuyWithLookup = async (lookup_address: PublicKey,creator:PublicKey,mint:PublicKey,wallets:Keypair[] = bundleBuyerKeys) => {
    const lookupTableAccount = (
      await connection.getAddressLookupTable(lookup_address)
    ).value;
  
    if (!lookupTableAccount) throw "no account find---";
    console.log("here is all address---", lookupTableAccount);


    const solAmount = 0.001;
    const tokenAmount = 17376518
  
    console.log("💰 Token amount to receive:", tokenAmount);
  
  
    const bundle_buy_itx = [];
  
    for (const keypair of wallets) {
      const tx1 = await sdk.getBuyTxs(
        //@ts-ignore
        GLOBAL_CONFIG,
        creator,
        mint,
        keypair.publicKey,
        100,
        new BN(tokenAmount),
        new BN(solAmount * LAMPORTS_PER_SOL)
      )
  
      if (tx1.success) {
        bundle_buy_itx.push(...tx1.data);
      }
    }
  
    console.log("helooooo---");
    let latestBlockhash = await connection.getLatestBlockhash();
  
    const messageV0 = new TransactionMessage({
      payerKey: wallets[0].publicKey,
      recentBlockhash: latestBlockhash.blockhash,
      instructions: [...bundle_buy_itx],
    }).compileToV0Message([lookupTableAccount]);
  
    const transaction = new VersionedTransaction(messageV0);
  
    transaction.sign([...wallets, wallets[0]]);
  
    try {
      const simulate = await connection.simulateTransaction(transaction);
      console.log("here is simulation res---", simulate);
  
      //  const sign = await connection.sendTransaction(transaction);
  
      //  console.log("here is the signature----", sign);
    } catch (e) {
      console.log("error--", e);
    }
  
    console.log(
      "With ALT the Bundle transaction size is -----",
      transaction.serialize().byteLength
    );
  };