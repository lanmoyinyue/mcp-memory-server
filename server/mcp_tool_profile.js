export const MEMORY_CHAT_TOOLS = Object.freeze([
  'write_memory',
  'read_memories',
  'update_memory',
  'search_memories',
  'hybrid_search',
  'delete_memory',
  'recall_lmc',
  'get_evidence',
  'find_related',
  'surface_spontaneous_memory',
  'somatic_ignite',
  'somatic_snapshot',
  'get_tide_status',
  'record_intimacy_event',
]);

const CHAT_TOOL_SET = new Set(MEMORY_CHAT_TOOLS);

export function normalizeMcpToolProfile(value) {
  return String(value || '').trim().toLowerCase() === 'chat' ? 'chat' : 'admin';
}

export function mcpToolProfileFromRequest(req) {
  return normalizeMcpToolProfile(
    req?.query?.profile
      || req?.headers?.['x-memory-mcp-profile']
      || process.env.MCP_TOOL_PROFILE
  );
}

export function installMcpToolProfile(mcp, profile) {
  const normalized = normalizeMcpToolProfile(profile);
  if (normalized !== 'chat') return mcp;

  const registerTool = mcp.tool.bind(mcp);
  mcp.tool = (name, ...args) => {
    if (!CHAT_TOOL_SET.has(name)) return undefined;
    return registerTool(name, ...args);
  };
  return mcp;
}
