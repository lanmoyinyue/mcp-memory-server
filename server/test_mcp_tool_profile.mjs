import assert from 'node:assert/strict';
import {
  MEMORY_CHAT_TOOLS,
  installMcpToolProfile,
  mcpToolProfileFromRequest,
  normalizeMcpToolProfile,
} from './mcp_tool_profile.js';

assert.equal(MEMORY_CHAT_TOOLS.length, 15);
assert.equal(new Set(MEMORY_CHAT_TOOLS).size, 15);
assert.equal(normalizeMcpToolProfile('chat'), 'chat');
assert.equal(normalizeMcpToolProfile('CHAT'), 'chat');
assert.equal(normalizeMcpToolProfile('admin'), 'admin');
assert.equal(normalizeMcpToolProfile('unknown'), 'admin');
assert.equal(mcpToolProfileFromRequest({ query: { profile: 'chat' }, headers: {} }), 'chat');
assert.equal(mcpToolProfileFromRequest({ query: {}, headers: { 'x-memory-mcp-profile': 'chat' } }), 'chat');

function fakeMcp() {
  const registered = [];
  return {
    registered,
    tool(name) {
      registered.push(name);
      return { name };
    },
  };
}

const chat = fakeMcp();
installMcpToolProfile(chat, 'chat');
for (const name of MEMORY_CHAT_TOOLS) chat.tool(name);
chat.tool('run_memory_patrol');
chat.tool('list_memory_patrol_reports');
assert.deepEqual(chat.registered, MEMORY_CHAT_TOOLS);

const admin = fakeMcp();
installMcpToolProfile(admin, 'admin');
admin.tool('write_memory');
admin.tool('run_memory_patrol');
assert.deepEqual(admin.registered, ['write_memory', 'run_memory_patrol']);

console.log('mcp tool profile tests passed');
