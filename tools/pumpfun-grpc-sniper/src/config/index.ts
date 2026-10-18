import { clusterApiUrl, Connection } from "@solana/web3.js"

export const RPCS_ENDPOINTS = [
    "https://mainnet.helius-rpc.com/?api-key=",
    "https://solana-mainnet.g.alchemy.com/v2/",
    "https://mainnet.helius-rpc.com/?api-key=",
    "https://mainnet.helius-rpc.com/?api-key=", 
    "https://mainnet.helius-rpc.com/?api-key=", 
    "https://mainnet.helius-rpc.com/?api-key=", 
    "https://mainnet.helius-rpc.com/?api-key=", 
]

export const connection = new Connection(clusterApiUrl("mainnet-beta"),"confirmed");