import {
  AddressLookupTableProgram,
  clusterApiUrl,
  Connection,
  Keypair,
  LAMPORTS_PER_SOL,
  PublicKey,
  SystemProgram,
  SYSVAR_RENT_PUBKEY,
  Transaction,
  TransactionInstruction,
  TransactionMessage,
  VersionedTransaction,
} from "@solana/web3.js";
import bs58 from "bs58";
import dotenv from "dotenv";
import BN from "bn.js";
import { createAndSendV0Tx } from "../executor/legacy";
import { createLUT } from "./createLUT";
import {
  ASSOCIATED_TOKEN_PROGRAM_ID,
  getAssociatedTokenAddress,
  TOKEN_PROGRAM_ID,
} from "@solana/spl-token";
import { connection } from "../config";
dotenv.config();

const signer = Keypair.fromSecretKey(bs58.decode(process.env.SIGNER_KEY!));

const program_id = new PublicKey("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P");

const lookupTxs = async (lookup_address: PublicKey, mint: PublicKey, walletKPs: Keypair[], mainKp: Keypair,creator:PublicKey) => {

  for (let i = 0; i < walletKPs.length; i++) {
    const balance = await connection.getBalance(walletKPs[i].publicKey);

    console.log(
      "here is teh balance of: " + walletKPs[i].publicKey.toBase58(),
      " ",
      balance / LAMPORTS_PER_SOL
    );
  }

  console.log("here is teh lookup address---", lookup_address?.toBase58());

  if (!lookup_address) throw "lookup account creation failed!!";

  const lookupTableAccount = (
    await connection.getAddressLookupTable(lookup_address)
  ).value;

  if (lookupTableAccount == null) {
    console.log("Lookup table account not found!");
  }

  const accounts: PublicKey[] = [];

  // fetching all accounts for the lookup inster
  const global = new PublicKey("4wTV1YmiEkRvAtNtsSGPtUrqRYQMe5SKy2uB4Jjaxnjf");
  const fee_recipent = new PublicKey(
    "AVmoTthdrX6tKt4nDjco2D775W2YK3sDhxPcMmzUAmTY"
  );
  const bonding_curve = PublicKey.findProgramAddressSync(
    [Buffer.from("bonding-curve"), mint.toBuffer()],
    program_id
  )[0];
  const associated_bonding_curve = await getAssociatedTokenAddress(
    mint,
    bonding_curve,
    true
  );
  const user_ata = await getAssociatedTokenAddress(mint, signer.publicKey);
  const creator_vault = PublicKey.findProgramAddressSync(
    [Buffer.from("creator-vault"), creator.toBuffer()],
    program_id
  )[0];
  const event_authority = new PublicKey(
    "Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F1"
  );
  const global_volume_accumulator = new PublicKey(
    "Hq2wp8uJ9jCPsYgNHex8RtqdvMPfVGoYwjvF1ATiwn2Y"
  );
  const user_volume_accumulator = new PublicKey(
    "28GfGdLbF6o2qmTuBLrr5HUnYJdUsj8ZARDu26hQRyrG"
  );
  const fee_config = new PublicKey(
    "8Wf5TiAheLUqBrKXeYg2JtAFFMWtKdG2BSFgqUcPVwTt"
  );

  accounts.push(
    ASSOCIATED_TOKEN_PROGRAM_ID,
    TOKEN_PROGRAM_ID,
    event_authority,
    global,
    program_id,
    associated_bonding_curve,
    bonding_curve,
    SystemProgram.programId,
    SYSVAR_RENT_PUBKEY,
    signer.publicKey,
    fee_config,
    fee_recipent,
    global_volume_accumulator,
    user_volume_accumulator,
    user_ata,
    creator_vault
  );

  // adding all the atas of all signers
  for (const keypair of walletKPs) {
    const user_ata = await getAssociatedTokenAddress(mint, keypair.publicKey);
    accounts.push(user_ata);
  }

  const extendInstruction = AddressLookupTableProgram.extendLookupTable({
    lookupTable: lookup_address,
    authority: signer.publicKey,
    payer: signer.publicKey,
    addresses: accounts,
  });

  await createAndSendV0Tx([extendInstruction], signer, connection);
};