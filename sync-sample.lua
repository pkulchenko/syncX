require "wx"
local unpack = table.unpack or unpack
local frame = wx.wxFrame(wx.NULL, wx.wxID_ANY, "syncX demo",
  wx.wxDefaultPosition, wx.wxSize(1020, 800), wx.wxDEFAULT_FRAME_STYLE)
local autosync = {on = "Autosync: ON", off = "Autosync: OFF"}
local statusbar = frame:CreateStatusBar(1)
statusbar:SetStatusStyles({wx.wxSB_FLAT})
statusbar:SetStatusWidths({-1})
statusbar:SetStatusText(autosync.on, 0)

local mgr = wxaui.wxAuiManager()
mgr:SetManagedWindow(frame)

local onidle = {}
local function doWhenIdle(func) table.insert(onidle, func) end
frame:Connect(wx.wxEVT_IDLE, function() if #onidle > 0 then table.remove(onidle, 1)() end end)

-- describe two editors to have content and two to host the logs
local editors = { editor1 = true, editor2 = true, log1 = true, log2 = true, graph1 = true, graph2 = true }
local indicators = {
  {wxstc.wxSTC_INDIC_ROUNDBOX, wx.wxColour(0, 150, 0)}, addtext = 1,
  {wxstc.wxSTC_INDIC_POINT, wx.wxColour(0, 0, 150)}, othercursor = 2,
  {wxstc.wxSTC_INDIC_FULLBOX, wx.wxColour(0, 0, 150)}, otherselection = 3,
  {wxstc.wxSTC_INDIC_STRIKE, wx.wxColour(200, 0, 0)}, delnode = 4,
}
local id = 100
local function getid() id = id + 1 return id end
local function createPane(name)
  local ed = wxstc.wxStyledTextCtrl(frame, getid(), wx.wxDefaultPosition, wx.wxSize(500, 400), wx.wxBORDER_NONE)
  local fontsize = 14
  if not name:find("editor") then
    ed:SetReadOnly(true)
    fontsize = name:find("log") and 12 or 10
  end
  local font = wx.wxFont(fontsize, wx.wxFONTFAMILY_MODERN, wx.wxFONTSTYLE_NORMAL, wx.wxFONTWEIGHT_NORMAL, false, "Courier New")
  ed:StyleSetFont(wxstc.wxSTC_STYLE_DEFAULT, font)
  ed:SetEOLMode(wxstc.wxSTC_EOL_LF) -- force LF as the line separator
  ed:SetWrapMode(wxstc.wxSTC_WRAP_WORD)

  if name:find("editor") then
    ed:SetMarginType(0, wxstc.wxSTC_MARGIN_NUMBER)
    ed:SetMarginMask(0, 0)
    ed:SetMarginWidth(0, 36)
  elseif name:find("log") then
    ed:SetMarginType(0, wxstc.wxSTC_MARGIN_SYMBOL)
    ed:SetMarginSensitive(0, true)
    ed:SetMarginWidth(0, 36)
  elseif name:find("graph") then -- graph editor-specific logic
    local foldtypes = {
      [0] = { wxstc.wxSTC_MARKNUM_FOLDEROPEN, wxstc.wxSTC_MARKNUM_FOLDER,
        wxstc.wxSTC_MARKNUM_FOLDERSUB, wxstc.wxSTC_MARKNUM_FOLDERTAIL, wxstc.wxSTC_MARKNUM_FOLDEREND,
        wxstc.wxSTC_MARKNUM_FOLDEROPENMID, wxstc.wxSTC_MARKNUM_FOLDERMIDTAIL,
      },
      fold = { wxstc.wxSTC_MARK_ARROWDOWN, wxstc.wxSTC_MARK_ARROW },
    }
    ed:SetMarginType(0, wxstc.wxSTC_MARGIN_SYMBOL)
    ed:SetMarginMask(0, wxstc.wxSTC_MASK_FOLDERS)
    ed:SetMarginSensitive(0, true)
    ed:SetMarginWidth(0, 36)
    ed:SetAutomaticFold(7)
    local fg, bg = wx.wxWHITE, wx.wxColour(128, 128, 128)
    for m = 1, #foldtypes[0] do
      ed:MarkerDefine(foldtypes[0][m], foldtypes.fold[m] or wxstc.wxSTC_MARK_EMPTY, fg, bg)
    end
  end

  -- set indicator for the current editor
  for name, index in pairs(indicators) do
    if type(index) == "number" then
      local itype, icolor = unpack(indicators[index])
      ed:IndicatorSetStyle(index, itype)
      ed:IndicatorSetForeground(index, icolor)
    end
  end

  local pi = wxaui.wxAuiPaneInfo():
  Name(name):CaptionVisible(false):
  PaneBorder(true):Fixed(true):MinSize(500,200):MaxSize(500,200):CloseButton(false)
  if name:find("1") then pi:Center() else pi:Right() end
  pi:Position(name:find("editor") and 0 or name:find("log") and 1 or 2)
  mgr:AddPane(ed, pi)
  ed.version = 0 -- set initial version
  return ed
end

-- assign editor objects
for name in pairs(editors) do editors[name] = createPane(name) end
mgr:Update()

local function writelog(log, str)
  log:SetReadOnly(false)
  log:AppendText(str)
  log:GotoPos(log:GetLength())
  log:SetReadOnly(true)
end

local function getnewversion(editor)
  editor.version = editor.version + 1
  return ("v%x_%x"):format(editor.version, editor:GetId())
end

local function showgraph(editor)
  -- update the graph representation
  editor.graph:SetReadOnly(false)
  editor.graph:ClearAll()
  editor.sync:walkgraph(function(args)
      local greditor = editor.graph
      local pos = greditor:GetLength()
      local text = ("%s%s: %q\n"):format((" "):rep(args.level), args.version, args.value:gsub("\n","\013"))
      greditor:AppendText(text)
      -- set the indicator for deleted nodes
      if args.isdeleted then
        greditor:SetIndicatorCurrent(indicators.delnode)
        greditor:IndicatorFillRange(pos+args.level, #text-args.level)
      end
      -- each line has a new line and first index is 0, so the last added line is total-2
      local lineidx = greditor:GetLineCount()-2
      local level = args.level + wxstc.wxSTC_FOLDLEVELBASE
      -- if the previous line has a different level, then make it a header
      if lineidx > 0 and greditor:GetFoldLevel(lineidx-1) < level then
        -- decrease the level on the previous one comparing to the current one
        greditor:SetFoldLevel(lineidx-1, level - 1 + wxstc.wxSTC_FOLDLEVELHEADERFLAG)
      end
      greditor:SetFoldLevel(lineidx, level)
    end)
  editor.graph:SetReadOnly(true)
end

-- setup syncX structures to keep track of editor changes
local function setsync(editor)
  -- initialize the editor
  local text = "Editor text"
  editor:SetText(text)
  editor:EmptyUndoBuffer()
  -- initialize the sync
  local sync9 = require "sync9"
  local resource = sync9.createresource(getnewversion(editor), text)
  resource:sethandler {
    version = function(resource, version)
      local origin = tonumber(version:match("_(.+)") or 0, 16)
      for _, patch in ipairs(resource:getpatchset(version)) do
        local addidx, delcnt, value = unpack(patch)
        if editor:GetId() ~= origin then -- remote update, apply the changes
          -- disable event handling, so that external updates don't trigger sync processing
          editor:SetEvtHandlerEnabled(false)
          -- make a replacement
          editor:SetTargetRange(addidx, addidx+delcnt)
          editor:ReplaceTarget(value or "")
          -- add modification indicator
          editor:SetIndicatorCurrent(indicators.addtext)
          editor:IndicatorFillRange(addidx, value and #value or 0)
          editor:SetEvtHandlerEnabled(true)
        end
      end

      doWhenIdle(function() showgraph(editor) end)
    end,
  }
  editor.sync = resource
  -- add here any custom initialization to display
  -- for example, `resource:addversion("v1", {{0, 15, 'New text'}})`
  -- show the current space graph
  showgraph(editor)
end

-- update editors to link their logs and their syncX trackers
editors.editor1.graph = editors.graph1
editors.editor2.graph = editors.graph2
editors.editor1.log = editors.log1
editors.editor2.log = editors.log2

setsync(editors.editor1)
setsync(editors.editor2)

editors.log1.sync = editors.editor2.sync
editors.log2.sync = editors.editor1.sync

-- set editor handlers to track modifications
local function editormodified(event)
  local evtype = event:GetModificationType()
  local pos = event:GetPosition()
  local length = event:GetLength()
  local editor = event:GetEventObject():DynamicCast("wxStyledTextCtrl")
  local inserted = bit.band(evtype, wxstc.wxSTC_MOD_BEFOREINSERT) ~= 0
  local deleted = bit.band(evtype, wxstc.wxSTC_MOD_BEFOREDELETE) ~= 0
  if not inserted and not deleted then return end
  local version = getnewversion(editor)
  local text = event:GetText()
  if inserted then
    -- color added text with the default color
    editor:SetIndicatorCurrent(indicators.addtext)
    editor:IndicatorClearRange(pos, length)
  end

  -- don't need to specify the parents, as all "future parents" will be used by default joining them together
  editor.sync:addversion(version, {{pos, inserted and 0 or length, inserted and text or ""}})

  local parents = {}
  for ver in pairs(editor.sync:getparents(version)) do table.insert(parents, ver .. " = true") end
  writelog(editor.log, ("%s, {%s,%s,%q}, {%s}"):format(version, pos, inserted and 0 or length,
      inserted and text or '', table.concat(parents, ", "))
    :gsub(',{""}', ""):gsub("\010","n"):gsub("\026","\\026").."\n")
end
editors.editor1:Connect(wxstc.wxEVT_STC_MODIFIED, editormodified)
editors.editor2:Connect(wxstc.wxEVT_STC_MODIFIED, editormodified)

-- set log handlers to send events
local function logclicked(event)
  local log = event:GetEventObject():DynamicCast("wxStyledTextCtrl")
  local line = log:GetLine(0)
  if line == "" then return end
  local version, patchstr, parentstr = line:match("(.-),%s*(%b{}),%s*(%b{})")
  log:SetReadOnly(false)
  log:DeleteRange(0, log:PositionFromLine(1))
  log:SetReadOnly(true)
  local patch, parents = (loadstring or load)("return "..patchstr..", "..parentstr)()
  log.sync:addversion(version, {patch}, parents)

  -- check if both logs are empty and purge both histories
  if editors.log1:GetText() == "" and editors.log2:GetText() == "" then
    for ed in pairs({editor1 = true, editor2 = true}) do
      editors[ed].sync:prune()
      showgraph(editors[ed])
    end
  end
end
editors.log1:Connect(wxstc.wxEVT_STC_MARGINCLICK, logclicked)
editors.log2:Connect(wxstc.wxEVT_STC_MARGINCLICK, logclicked)

local function synconidle(event) if statusbar:GetStatusText(0) == autosync.on then logclicked(event) end end
editors.log1:Connect(wx.wxEVT_IDLE, synconidle)
editors.log2:Connect(wx.wxEVT_IDLE, synconidle)

local cabinet = require "cabinet"
editors.editor1.cab = cabinet.publish("editor1")
editors.editor2.cab = cabinet.publish("editor2")
local function editorupdateui(event)
  if event:GetUpdated() ~= wxstc.wxSTC_UPDATE_SELECTION then return end
  local editor = event:GetEventObject():DynamicCast("wxStyledTextCtrl")
  editor.cab.versions = editor.sync:getparents()
  editor.cab.cursor = editor:GetAnchor()
  editor.cab.sels = editor:GetSelectionStart()
  editor.cab.sele = editor:GetSelectionEnd()
end
editors.editor1:Connect(wxstc.wxEVT_STC_UPDATEUI, editorupdateui)
editors.editor2:Connect(wxstc.wxEVT_STC_UPDATEUI, editorupdateui)

local function updateeditor(ed)
  local function update(t, key)
    if key == "cursor" then
      ed:SetIndicatorCurrent(indicators.othercursor)
      ed:IndicatorClearRange(0, ed:GetLength())
      ed:IndicatorFillRange(ed.sync:getindex(t.versions, t.cursor), 1)
    else
      ed:SetIndicatorCurrent(indicators.otherselection)
      ed:IndicatorClearRange(0, ed:GetLength())
      if t.sels and t.sele then
        -- selection can be done left to right or right to left,
        -- but indicators are drawn only left to right,
        -- so reverse the selection if needed
        local sels = ed.sync:getindex(t.versions, math.min(t.sels, t.sele))
        local sele = ed.sync:getindex(t.versions, math.max(t.sels, t.sele))
        ed:IndicatorFillRange(sels, sele-sels)
      end
    end
  end
  return update
end
cabinet.subscribe("editor1", updateeditor(editors.editor2))
cabinet.subscribe("editor2", updateeditor(editors.editor1))

statusbar:Connect(wx.wxEVT_LEFT_DOWN, function ()
    statusbar:SetStatusText(statusbar:GetStatusText(0) == autosync.on and autosync.off or autosync.on, 0)
  end)

frame:Connect(wx.wxEVT_CLOSE_WINDOW, function(event) mgr:UnInit() event:Skip() os.exit() end)
frame:SetMinSize(frame:GetSize())
frame:Show(true)
-- set the focus on the first editor at the first opportunity
doWhenIdle(function() editors.editor1:SetFocus() end)
wx.wxGetApp():MainLoop()
