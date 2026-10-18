import Client, { CommitmentLevel, SubscribeRequest, SubscribeUpdate } from "@triton-one/yellowstone-grpc";
import { PublicKey } from "@solana/web3.js";
import bs58 from "bs58";
import dotenv from "dotenv";
import { BorshInstructionCoder } from "@coral-xyz/anchor";
import pumpIdl from "../IDL/pump.json"
import { SnipeToken } from "./snipe";

dotenv.config();

//@ts-ignore
const coder = new BorshInstructionCoder(pumpIdl);


const ENDPOINT = process.env.GRPC_ENDPOINT!;
const PUMP_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";


const CREATE_DISCRIMINATOR = Buffer.from([24, 30, 200, 40, 5, 28, 7, 119]);


// Concurrency, timeout, and dedupe configuration
const MAX_CONCURRENCY = Number(process.env.SNIPE_CONCURRENCY || 4);
const SNIPE_TIMEOUT_MS = Number(process.env.SNIPE_TIMEOUT_MS || 8000);
const DEDUPE_TTL_MS = Number(process.env.DEDUPE_TTL_MS || 60000);

class ConcurrencyQueue {
  private active = 0;
  private queue: Array<() => Promise<void>> = [];
  constructor(private concurrency: number) {}

  add(task: () => Promise<void>): Promise<void> {
    return new Promise((resolve) => {
      const run = async () => {
        this.active++;
        try {
          await task();
        } finally {
          this.active--;
          this.next();
          resolve();
        }
      };
      this.queue.push(run);
      this.next();
    });
  }

  private next() {
    while (this.active < this.concurrency && this.queue.length) {
      const fn = this.queue.shift()!;
      fn();
    }
  }
}

function withTimeout<T>(promise: Promise<T>, ms: number): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const id = setTimeout(() => reject(new Error("snipe timeout")), ms);
    promise.then(
      (v) => {
        clearTimeout(id);
        resolve(v);
      },
      (e) => {
        clearTimeout(id);
        reject(e);
      }
    );
  });
}

const snipeQueue = new ConcurrencyQueue(MAX_CONCURRENCY);
const seenMints = new Map<string, number>();

function shouldProcessMint(mint: string): boolean {
  const now = Date.now();
  const last = seenMints.get(mint) || 0;
  if (now - last < DEDUPE_TTL_MS) return false;
  seenMints.set(mint, now);
  return true;
}

// Periodic cleanup of dedupe map
setInterval(() => {
  const now = Date.now();
  for (const [mint, ts] of seenMints) {
    if (now - ts > DEDUPE_TTL_MS) seenMints.delete(mint);
  }
  // @ts-ignore - unref might not exist in browser-like envs
}, DEDUPE_TTL_MS).unref?.();

interface Args {
   name: string
   data: {
    name: string
    symbol: string
    uri: string
    creator: PublicKey
   }
}

async function main() {
  console.log("Starting Pump.fun token listener...");
  
  const client = new Client(ENDPOINT,undefined,undefined);
  const stream = await client.subscribe();

  const request: SubscribeRequest = {
    accounts: {},
    slots: {},
    transactions: {
      pumpFun: {
        accountInclude: [PUMP_PROGRAM_ID],
        accountExclude: [],
        accountRequired: [],
      },
    },
    transactionsStatus: {},
    entry: {},
    blocks: {},
    blocksMeta: {},
    commitment: CommitmentLevel.PROCESSED,
    accountsDataSlice: [],
    ping: undefined,
  };

  stream.write(request);
  console.log("✓ Listening for new Pump.fun tokens...\n");

  stream.on("data", async (data: SubscribeUpdate) => {
    const transaction = data.transaction?.transaction;
    const message = transaction?.transaction?.message;
    
    if (!transaction || !message) return;


    // method one
    const createInstruction = message.instructions.find(ix => 
      ix.data && CREATE_DISCRIMINATOR.equals(ix.data.slice(0, 8))
    );

    
      // method two
    //     const createInstruction = message.instructions.find(ix => {
    //     const descrimnator = ix.data.slice(0,8);

    //    if(CREATE_DISCRIMINATOR.equals(descrimnator)){
    //     console.log("created-----")
    //    }
         
    // });
    if (!createInstruction) return;

    try {
      const tokenData = await parseTokenData(message, createInstruction, transaction, data);
      if (tokenData) {

      console.log("─".repeat(50));


       console.log("🚀 NEW TOKEN DETECTED FROM PUMPFUN THROUGH GRPC! ---");

        console.log(tokenData);

      console.log("LISTEINING FOR OHTERS TOKENS -------------- ");

        console.log("─".repeat(50));
      }
    } catch (error) {
      console.error("Error parsing token data:", error);
    }
  });

  stream.on("error", (error) => console.error("Stream error:", error));
  stream.on("end", () => console.log("Stream ended"));
}

async function parseTokenData(message: any, instruction: any, transaction: any, data: any) {
  try {
    const accountKeys = message.accountKeys;
    const accounts = instruction.accounts;
    
    const mint = new PublicKey(accountKeys[accounts[0]]).toBase58();
    const bonding_curve = new PublicKey(accountKeys[accounts[2]]).toBase58();
    const associated_bonding_curve = new PublicKey(accountKeys[accounts[3]]).toBase58();
    const user = new PublicKey(accountKeys[accounts[7]]).toBase58();


    const decoded = coder.decode(instruction.data, "base58");

    if (!decoded) {
        return null;
    }

    const args: Args = decoded as Args;

    if (shouldProcessMint(mint)) {
      void snipeQueue.add(() =>
        withTimeout(
          SnipeToken(new PublicKey(mint), args.data.creator),
          SNIPE_TIMEOUT_MS
        ).catch((err) => {
          console.error("Snipe task failed:", err?.message || err);
        })
      );
    }

    return {
      signature: bs58.encode(Buffer.from(transaction.signature)),
      slot: data.transaction?.slot?.toString() || "",
      mint,
      name: args.data.name,
      uri: args.data.uri,
      symbol: args.data.symbol,
      creator: args.data.creator.toBase58(),
      bonding_curve,
      associated_bonding_curve,
      user,
    };
  } catch (error) {
    console.error("Error parsing token data:", error);
    return null;
  }
}


main().catch(console.error);