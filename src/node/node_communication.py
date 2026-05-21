"""
Node Communication - Komunikasi antar node dalam cluster
"""

import asyncio
import logging
import random
from typing import Dict, Any, Optional, Callable
from datetime import datetime

from src.config.system_config import SystemConfig
from src.node.node_manager import NodeManager

logger = logging.getLogger(__name__)


class NodeCommunication:
    """
    Kelas untuk mengelola komunikasi antar node
    Mensimulasikan network communication dengan berbagai fault
    """
    
    def __init__(self, config: SystemConfig, node_manager: NodeManager):
        """
        Inisialisasi node communication
        
        Args:
            config: Konfigurasi sistem
            node_manager: Manajer node
        """
        self.config = config
        self.node_manager = node_manager
        
        # Message handlers
        self.message_handlers: Dict[str, Callable] = {}
        
        # Message queue
        self.message_queue: asyncio.Queue = asyncio.Queue()
        
        # Statistics
        self.messages_sent = 0
        self.messages_received = 0
        self.messages_dropped = 0
        self.messages_corrupted = 0
        
        self.is_running = False
        self.processor_task: Optional[asyncio.Task] = None
        
        logger.info("NodeCommunication initialized")
    
    async def start(self):
        """Memulai komunikasi node"""
        self.is_running = True
        self.processor_task = asyncio.create_task(self._process_messages())
        
        logger.info("NodeCommunication started")
    
    async def stop(self):
        """Menghentikan komunikasi node"""
        self.is_running = False
        
        if self.processor_task:
            self.processor_task.cancel()
            try:
                await self.processor_task
            except asyncio.CancelledError:
                pass
        
        logger.info("NodeCommunication stopped")
    
    def register_handler(self, message_type: str, handler: Callable):
        """
        Register handler untuk tipe message tertentu
        
        Args:
            message_type: Tipe message
            handler: Fungsi handler
        """
        self.message_handlers[message_type] = handler
        logger.debug(f"Handler registered for message type: {message_type}")
    
    async def send_message(
        self,
        from_node: int,
        to_node: int,
        message_type: str,
        payload: Any,
        timeout_ms: int = None
    ) -> Optional[Any]:
        """
        Mengirim message ke node lain
        
        Args:
            from_node: ID node pengirim
            to_node: ID node penerima
            message_type: Tipe message
            payload: Payload message
            timeout_ms: Timeout dalam ms
            
        Returns:
            Response atau None jika timeout
        """
        # Simulasi network partition
        if self.node_manager.network_partition_active:
            groups = self.node_manager.partition_groups
            if groups:
                # Cek apakah pengirim dan penerima dalam partition berbeda
                from_in_a = from_node in groups[0]
                to_in_a = to_node in groups[0]
                
                if from_in_a != to_in_a:
                    logger.debug(f"Message dropped due to network partition: {from_node} -> {to_node}")
                    self.messages_dropped += 1
                    return None
        
        # Simulasi message loss
        if random.random() < self.node_manager.message_loss_probability:
            logger.debug(f"Message dropped due to loss: {from_node} -> {to_node}")
            self.messages_dropped += 1
            return None
        
        # Simulasi network latency
        latency = self.node_manager.network_latency_ms / 1000
        if latency > 0:
            await asyncio.sleep(latency)
        
        # Simulasi message corruption
        if random.random() < self.node_manager.corruption_probability:
            logger.debug(f"Message corrupted: {from_node} -> {to_node}")
            self.messages_corrupted += 1
            
            # Corrupt payload
            if isinstance(payload, dict):
                payload = self._corrupt_payload(payload)
        
        # Buat message
        message = {
            'id': f"{from_node}_{to_node}_{datetime.now().timestamp()}",
            'from_node': from_node,
            'to_node': to_node,
            'type': message_type,
            'payload': payload,
            'timestamp': datetime.now()
        }
        
        self.messages_sent += 1
        
        # Simulasi synchronous request-response
        if message_type.startswith('request_'):
            # Create response queue for this message
            response_queue = asyncio.Queue()
            message['response_queue'] = response_queue
            
            # Queue message for processing
            await self.message_queue.put(message)
            
            # Wait for response
            timeout = timeout_ms or self.config.network_timeout_ms
            try:
                response = await asyncio.wait_for(response_queue.get(), timeout=timeout/1000)
                return response
            except asyncio.TimeoutError:
                logger.warning(f"Request timeout: {from_node} -> {to_node} ({message_type})")
                return None
        
        else:
            # Async message, just queue
            await self.message_queue.put(message)
            return True
    
    async def _process_messages(self):
        """Memproses message queue"""
        logger.info("Message processor started")
        
        while self.is_running:
            try:
                # Get message from queue
                message = await asyncio.wait_for(
                    self.message_queue.get(),
                    timeout=1.0
                )
                
                # Process message
                await self._handle_message(message)
                
                # Mark as done
                self.message_queue.task_done()
                
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error processing message: {e}", exc_info=True)
    
    async def _handle_message(self, message: dict):
        """
        Handle incoming message
        
        Args:
            message: Message yang diterima
        """
        to_node = message['to_node']
        message_type = message['type']
        payload = message['payload']
        
        # Cek apakah message untuk node ini
        if to_node != self.node_manager.local_node_id:
            # Forward ke node yang tepat (implementasi routing)
            logger.debug(f"Forwarding message {message['id']} to node {to_node}")
            await self.message_queue.put(message)
            return
        
        self.messages_received += 1
        
        # Cari handler
        handler = self.message_handlers.get(message_type)
        
        if handler:
            try:
                # Call handler
                response = await handler(payload)
                
                # Kirim response jika ada response_queue
                if 'response_queue' in message:
                    await message['response_queue'].put(response)
                    
            except Exception as e:
                logger.error(f"Error in message handler for {message_type}: {e}")
                if 'response_queue' in message:
                    await message['response_queue'].put({'error': str(e)})
        else:
            logger.warning(f"No handler for message type: {message_type}")
            
            if 'response_queue' in message:
                await message['response_queue'].put({'error': 'No handler'})
    
    def _corrupt_payload(self, payload: dict) -> dict:
        """
        Corrupt payload untuk simulasi message corruption
        
        Args:
            payload: Payload original
            
        Returns:
            Payload yang sudah di-corrupt
        """
        if not isinstance(payload, dict):
            if isinstance(payload, (str, int, float)):
                return f"CORRUPTED_{payload}"
            return payload
        
        # Corrupt random field
        if payload and random.random() < 0.5:
            keys = list(payload.keys())
            if keys:
                key_to_corrupt = random.choice(keys)
                payload[key_to_corrupt] = f"CORRUPTED_{payload[key_to_corrupt]}"
        
        # Or add corrupt field
        if random.random() < 0.3:
            payload['__corrupted'] = True
        
        return payload
    
    def get_stats(self) -> dict:
        """
        Mendapatkan statistik komunikasi
        
        Returns:
            Dictionary statistik
        """
        return {
            'messages_sent': self.messages_sent,
            'messages_received': self.messages_received,
            'messages_dropped': self.messages_dropped,
            'messages_corrupted': self.messages_corrupted,
            'queue_size': self.message_queue.qsize(),
            'handlers_registered': len(self.message_handlers),
            'delivery_rate': (
                self.messages_received / self.messages_sent 
                if self.messages_sent > 0 else 0
            )
        }
    
    def reset_stats(self):
        """Reset statistik komunikasi"""
        self.messages_sent = 0
        self.messages_received = 0
        self.messages_dropped = 0
        self.messages_corrupted = 0