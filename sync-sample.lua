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

-- describe two editors to have content and two to host the logs
local editors = { editor1 = true, editor2 = true, log1 = true, log2 = true, graph1 = true, graph2 = true }
local indicators = {
  {wxstc.wxSTC_INDIC_ROUNDBOX, wx.wxColour(200, 0, 0)}, editor1 = 1,
  {wxstc.wxSTC_INDIC_ROUNDBOX, wx.wxColour(0, 150, 0)}, editor2 = 2,
  {wxstc.wxSTC_INDIC_STRIKE, wx.wxColour(200, 0, 0)}, graph1 = 3, graph2 = 3,
}
local id = 100
local function getid() id = id + 1 return id end
local function createPane(name)
  local ed = wxstc.wxStyledTextCtrl(frame, getid(), wx.wxDefaultPosition, wx.wxSize(500, 400), wx.wxBORDER_NONE)
  local fontsize = 14
  if name:find("editor") then -- editor-specific logic
    ed:SetText("Editor text")
    ed:EmptyUndoBuffer()
  else
    ed:SetReadOnly(true)
    fontsize = 12
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
  ed.indicator = indicators[name]
  if ed.indicator then
    local itype, icolor = unpack(indicators[ed.indicator])
    ed:IndicatorSetStyle(ed.indicator, itype)
    ed:IndicatorSetForeground(ed.indicator, icolor)
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

-- setup syncX structures to keep track of editor changes
local function setsync(editor)
  local sync9 = require "sync9"
  local data = setmetatable({n = editor:GetLength()}, {__index = {
        slice = function(tbl, i, j) return {n = (j or tbl.n) - i + 1} end,
        getlength = function(tbl) return tbl.n end,
        getvalue = function(tbl, offset) return editor:GetTextRange(offset, offset + tbl.n) end,
      }})
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
          greditor:SetIndicatorCurrent(greditor.indicator)
          greditor:IndicatorFillRange(pos+args.level, #text-args.level)
        end
        -- set folding
        local lines = greditor:GetLineCount()
        local level = args.level + wxstc.wxSTC_FOLDLEVELBASE
        -- if the previous line is a node or has a different level, then make it a header
        if args.isnode or lines > 1 and greditor:GetFoldLevel(lines-2) ~= level then
          greditor:SetFoldLevel(lines-2, level + wxstc.wxSTC_FOLDLEVELHEADERFLAG)
        end
        greditor:SetFoldLevel(lines-1, level)
      end)
    editor.graph:SetReadOnly(true)
  end
  local resource = sync9.createresource(getnewversion(editor), data)
  resource:sethandler {
    version = function(resource, version)
      local origin = tonumber(version:match("_(.+)"), 16)
      if editor:GetId() ~= origin then -- remote update, apply the changes
        for _, patch in ipairs(resource:getpatchset(version)) do
          local addidx, delcnt, value = unpack(patch)
          -- disable event handling, so that external updates don't trigger sync processing
          editor:SetEvtHandlerEnabled(false)
          if value and #value > 0 then
            editor:InsertText(addidx, value)
            editor:SetIndicatorCurrent(editor.indicator)
            editor:IndicatorFillRange(addidx, #value)
          end
          if delcnt > 0 then
            editor:DeleteRange(addidx, delcnt)
          end
          editor:SetEvtHandlerEnabled(true)
        end
      end
      showgraph(editor)
    end,
  }
  editor.sync = resource
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
  local inserted = bit.band(evtype, wxstc.wxSTC_MOD_INSERTTEXT) ~= 0
  local deleted = bit.band(evtype, wxstc.wxSTC_MOD_DELETETEXT) ~= 0
  if not inserted and not deleted then return end
  local version = getnewversion(editor)
  local text = editor:GetTextRange(pos, pos+length)
  if inserted then
    -- color added text with the default color
    editor:SetIndicatorCurrent(editor.indicator)
    editor:IndicatorClearRange(pos, length)
  end

  -- don't need to specify the parents, as all "future parents" will be used by default joining them together
  editor.sync:addversion(version, {{pos, inserted and 0 or length, inserted and text or {}}})

  local parents = {}
  for version in pairs(editor.sync:getparents(version)) do table.insert(parents, version .. " = true") end
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
end
editors.log1:Connect(wxstc.wxEVT_STC_MARGINCLICK, logclicked)
editors.log2:Connect(wxstc.wxEVT_STC_MARGINCLICK, logclicked)

local function synconidle(event) if statusbar:GetStatusText(0) == autosync.on then logclicked(event) end end
editors.log1:Connect(wx.wxEVT_IDLE, synconidle)
editors.log2:Connect(wx.wxEVT_IDLE, synconidle)

statusbar:Connect(wx.wxEVT_LEFT_DOWN, function (event)
    statusbar:SetStatusText(statusbar:GetStatusText(0) == autosync.on and autosync.off or autosync.on, 0)
  end)

frame:Connect(wx.wxEVT_CLOSE_WINDOW, function(event) mgr:UnInit() event:Skip() os.exit() end)
frame:SetMinSize(frame:GetSize())
frame:Show(true)
-- set the focus on the first editor at the first opportunity
frame:Connect(wx.wxEVT_IDLE, function() editors.editor1:SetFocus() frame:Disconnect(wx.wxEVT_IDLE) end)
wx.wxGetApp():MainLoop()
